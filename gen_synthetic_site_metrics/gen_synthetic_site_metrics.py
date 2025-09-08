import json, math, random, sys
from datetime import datetime, timedelta, timezone

if len(sys.argv) < 3:
    print("Usage: python generate_metrics_per_site.py input_sites.json output_with_metrics.json"); sys.exit(1)

inp, outp = sys.argv[1], sys.argv[2]
with open(inp, "r", encoding="utf-8") as f:
    sites = json.load(f)

now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
start = now - timedelta(days=3)
step = timedelta(minutes=15)

def series(func):
    t = start
    data = []
    i = 0
    while t <= now:
        data.append({"ts": t.isoformat().replace("+00:00","Z"), "val": round(func(i, t), 6)})
        t += step; i += 1
    return data

for s in sites:
    # skip sites without coords
    if s.get("latitude") is None or s.get("longitude") is None: 
        s["metrics"] = {}; continue

    # diurnal pattern helpers
    phase = random.random() * math.pi * 2
    def diurnal(i, t, base=0.4, amp=0.3):
        hour = (t.hour + t.minute/60.0)
        return max(0.0, min(1.0, base + amp * math.sin((hour/24.0)*2*math.pi + phase) + random.uniform(-0.05, 0.05)))

    cpu_util = series(lambda i,t: diurnal(i,t))
    mem_util = series(lambda i,t: 0.5 + 0.35*math.sin(i/48.0 + phase) + random.uniform(-0.05,0.05))
    mem_util = [{"ts":x["ts"], "val": max(0, min(1, x["val"]))} for x in mem_util]

    # bytes over 15 min; integrate simple varying throughput
    rx = series(lambda i,t: max(0, int(5e6 + 4e6*math.sin(i/32.0+phase) + random.gauss(0,2e6))))
    tx = series(lambda i,t: max(0, int(4e6 + 3e6*math.sin(i/28.0+phase) + random.gauss(0,1.5e6))))

    # power and energy (kWh) per 15 min
    power_w = series(lambda i,t: max(50.0, 200.0*diurnal(i,t,base=0.3,amp=0.5) + random.uniform(-10,10)))
    energy_kwh = [{"ts": x["ts"], "val": round(x["val"] * 0.25 / 1000.0, 6)} for x in power_w]  # 15 min slot

    s["metrics"] = {
        "cpu.util": cpu_util,
        "mem.util": mem_util,
        "net.rx.bytes": rx,
        "net.tx.bytes": tx,
        "power.w": power_w,
        "energy.kwh": energy_kwh
    }

with open(outp, "w", encoding="utf-8") as f:
    json.dump(sites, f, ensure_ascii=False, indent=2)
print(f"Wrote {outp}")
