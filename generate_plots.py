"""
generate_plots.py — Run load tests + collect metrics + generate all report plots.

Generates:
  plots/plot1_response_time_vs_concurrency.png
  plots/plot2_backend_scores_over_time.png
  plots/plot3_cpu_utilization.png
  plots/plot4_scores_vs_concurrency.png

Usage:
    python3 generate_plots.py
"""
import time, json, threading, urllib.request, urllib.parse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import paramiko

LB_URL   = "http://10.1.75.53:6205"
SSH_HOST = "10.1.75.53"
SSH_USER = "student"
SSH_PASS = "abhi1patel"
SYSTEMS  = {
    "Sys1-LB": 2205,
    "Sys2-BE": 2206,
    "Sys3-BE": 2207,
    "Sys4-BE": 2208,
}
PLOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

# ── Helpers ────────────────────────────────────────────────────────────────
def post_message(user, msg, timeout=8):
    body = urllib.parse.urlencode({"client-name": user, "msg": msg}).encode()
    req = urllib.request.Request(
        f"{LB_URL}/message", data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return (time.time() - t0) * 1000, True
    except Exception:
        return (time.time() - t0) * 1000, False

def get_feed(timeout=6):
    t0 = time.time()
    try:
        with urllib.request.urlopen(f"{LB_URL}/feed", timeout=timeout):
            return (time.time() - t0) * 1000, True
    except Exception:
        return (time.time() - t0) * 1000, False

def get_lb_status():
    try:
        with urllib.request.urlopen(f"{LB_URL}/lb/status", timeout=5) as r:
            return json.loads(r.read())
    except Exception:
        return None

def ssh_cpu(ssh_port):
    """Return CPU usage % from /proc/stat sample pair."""
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(SSH_HOST, port=ssh_port, username=SSH_USER,
                  password=SSH_PASS, timeout=6)
        def read_stat(client):
            _, out, _ = client.exec_command(
                "cat /proc/stat | grep '^cpu '", timeout=4)
            parts = out.read().decode().split()
            vals = list(map(int, parts[1:8]))
            idle = vals[3]
            total = sum(vals)
            return idle, total
        idle1, total1 = read_stat(c)
        time.sleep(0.5)
        idle2, total2 = read_stat(c)
        c.close()
        d_idle = idle2 - idle1
        d_total = total2 - total1
        if d_total == 0:
            return 0.0
        return round(100.0 * (1 - d_idle / d_total), 1)
    except Exception:
        return None

# ── Plot 1: Response time vs concurrency ──────────────────────────────────
def run_at_concurrency(concurrency, n_requests=100):
    users = [f"User{i%20}" for i in range(n_requests)]
    latencies = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(post_message, users[i], f"msg {i} c{concurrency}")
                   for i in range(n_requests)]
        for f in as_completed(futures):
            lat, ok = f.result()
            if ok:
                latencies.append(lat)
    return sorted(latencies)

def plot1():
    print("\n[Plot 1] Response time vs concurrency — running 4 load levels...")
    levels = [5, 10, 20, 40]
    means, medians, p95s, p99s = [], [], [], []

    for c in levels:
        print(f"  c={c:2d} users ...", end=" ", flush=True)
        lats = run_at_concurrency(c, n_requests=max(60, c*5))
        if not lats:
            means.append(0); medians.append(0); p95s.append(0); p99s.append(0)
            print("no data")
            continue
        n = len(lats)
        means.append(round(sum(lats)/n, 1))
        medians.append(lats[n//2])
        p95s.append(lats[int(n*0.95)])
        p99s.append(lats[min(int(n*0.99), n-1)])
        print(f"mean={means[-1]:.0f}ms  p95={p95s[-1]:.0f}ms  n={n}")
        time.sleep(4)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(levels, means,   "o-", lw=2, label="Mean")
    ax.plot(levels, medians, "s-", lw=2, label="Median")
    ax.plot(levels, p95s,    "^-", lw=2, label="p95")
    ax.plot(levels, p99s,    "D-", lw=2, label="p99")
    ax.set_xlabel("Concurrent Users")
    ax.set_ylabel("Response Time (ms)")
    ax.set_title("Response Time vs. Concurrency Level")
    ax.set_xticks(levels)
    ax.legend()
    ax.grid(True, alpha=0.3)
    path = os.path.join(PLOTS_DIR, "plot1_response_time_vs_concurrency.png")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path

# ── Plot 2: Backend scores over time during load ───────────────────────────
def plot2():
    print("\n[Plot 2] Backend scores over time — 60s load test...")
    scores = {"Sys2": [], "Sys3": [], "Sys4": []}
    timestamps = []
    stop = threading.Event()

    def worker():
        i = 0
        while not stop.is_set():
            post_message(f"U{i%20}", f"load {i}", timeout=4)
            if i % 3 == 0:
                get_feed(timeout=4)
            i += 1
            time.sleep(0.06)

    def poller():
        t0 = time.time()
        while not stop.is_set():
            st = get_lb_status()
            if st:
                timestamps.append(round(time.time() - t0, 1))
                for b in st.get("backends", []):
                    if b["id"] in scores:
                        scores[b["id"]].append(b["performance_score"])
            time.sleep(2)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(12)]
    threads.append(threading.Thread(target=poller, daemon=True))
    for t in threads:
        t.start()
    time.sleep(60)
    stop.set()
    for t in threads:
        t.join(timeout=3)

    n = min(len(timestamps), min(len(v) for v in scores.values()))
    ts = timestamps[:n]

    fig, ax = plt.subplots(figsize=(9, 5))
    for node, vals in scores.items():
        ax.plot(ts, vals[:n], lw=1.8, label=node)
    ax.axhline(500, color="red", ls="--", lw=1.2, label="Threshold (500)")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Performance Score")
    ax.set_title("Backend Performance Scores During Load Test")
    ax.legend()
    ax.grid(True, alpha=0.3)
    path = os.path.join(PLOTS_DIR, "plot2_backend_scores_over_time.png")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path

# ── Plot 3: CPU across all 4 systems ──────────────────────────────────────
def plot3():
    print("\n[Plot 3] CPU utilization across all 4 systems — 60s...")
    cpu_data = {name: [] for name in SYSTEMS}
    timestamps = []
    stop = threading.Event()

    def worker():
        i = 0
        while not stop.is_set():
            post_message(f"C{i%15}", f"cpu test {i}", timeout=4)
            get_feed(timeout=4)
            i += 1
            time.sleep(0.07)

    def collector():
        t0 = time.time()
        while not stop.is_set():
            row_ts = round(time.time() - t0, 1)
            for name, port in SYSTEMS.items():
                cpu = ssh_cpu(port)
                cpu_data[name].append(cpu if cpu is not None else 0.0)
            timestamps.append(row_ts)
            time.sleep(4)

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(15)]
    col = threading.Thread(target=collector, daemon=True)
    for w in workers:
        w.start()
    col.start()
    time.sleep(60)
    stop.set()
    col.join(timeout=8)

    n = min(len(timestamps), min(len(v) for v in cpu_data.values()))
    ts = timestamps[:n]

    fig, ax = plt.subplots(figsize=(10, 5))
    styles = ["-", "--", "-.", ":"]
    for i, (name, vals) in enumerate(cpu_data.items()):
        ax.plot(ts, vals[:n], lw=2, ls=styles[i], label=name)
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("CPU Utilization (%)")
    ax.set_title("CPU Utilization Across All 4 Systems During Load Test")
    ax.set_ylim(0, 100)
    ax.legend()
    ax.grid(True, alpha=0.3)
    path = os.path.join(PLOTS_DIR, "plot3_cpu_utilization.png")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path

# ── Plot 4: Backend scores bar chart at each concurrency ──────────────────
def plot4():
    print("\n[Plot 4] Backend scores at concurrency 5, 10, 20, 40...")
    levels = [5, 10, 20, 40]
    all_scores = {n: [] for n in ["Sys2", "Sys3", "Sys4"]}
    stop = threading.Event()

    def worker(c):
        i = 0
        while not stop.is_set():
            post_message(f"S{i%20}", f"sc test {i}", timeout=4)
            i += 1
            time.sleep(0.05)

    for c in levels:
        print(f"  c={c} ...", end=" ", flush=True)
        stop.clear()
        workers = [threading.Thread(target=worker, args=(c,), daemon=True)
                   for _ in range(c)]
        for w in workers:
            w.start()
        time.sleep(8)
        samples = {"Sys2": [], "Sys3": [], "Sys4": []}
        for _ in range(6):
            st = get_lb_status()
            if st:
                for b in st.get("backends", []):
                    if b["id"] in samples:
                        samples[b["id"]].append(b["performance_score"])
            time.sleep(1)
        stop.set()
        for w in workers:
            w.join(timeout=3)
        avg = lambda lst: round(sum(lst)/len(lst), 1) if lst else 0.0
        for node in all_scores:
            all_scores[node].append(avg(samples[node]))
        print({n: all_scores[n][-1] for n in all_scores})
        time.sleep(3)

    x = list(range(len(levels)))
    w = 0.25
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar([i - w for i in x], all_scores["Sys2"], w, label="Sys2")
    ax.bar([i     for i in x], all_scores["Sys3"], w, label="Sys3")
    ax.bar([i + w for i in x], all_scores["Sys4"], w, label="Sys4")
    ax.axhline(500, color="red", ls="--", lw=1.2, label="Threshold (500)")
    ax.set_xlabel("Concurrent Users")
    ax.set_ylabel("Avg Performance Score")
    ax.set_title("Backend Performance Scores at Different Concurrency Levels")
    ax.set_xticks(x)
    ax.set_xticklabels(levels)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    path = os.path.join(PLOTS_DIR, "plot4_scores_vs_concurrency.png")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path

# ── Main ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*60)
    print("Generating all report plots (~5 minutes total)")
    print("="*60)

    paths = []
    paths.append(plot1())   # ~2 min
    paths.append(plot2())   # ~1 min
    paths.append(plot3())   # ~1 min
    paths.append(plot4())   # ~1 min

    print("\n" + "="*60)
    print("DONE. Plots saved:")
    for p in paths:
        print(f"  {p}")
    print()
    print("HTOP SCREENSHOTS — open 4 terminals, run load test, screenshot each:")
    print()
    print("  Terminal 1 (load test):")
    print("    python3 load_generator/generator.py \\")
    print("      --url http://10.1.75.53:6205 \\")
    print("      --requests 500 --concurrency 30 --scenario mixed")
    print()
    print("  Terminal 2: ssh -p 2205 student@10.1.75.53  -> htop  (Sys1 LB)")
    print("  Terminal 3: ssh -p 2206 student@10.1.75.53  -> htop  (Sys2 backend)")
    print("  Terminal 4: ssh -p 2207 student@10.1.75.53  -> htop  (Sys3 backend)")
    print("  Terminal 5: ssh -p 2208 student@10.1.75.53  -> htop  (Sys4 backend)")
    print("  Password all: abhi1patel")
    print("="*60)
