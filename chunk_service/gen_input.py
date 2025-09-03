import json, random, os, time
from datetime import datetime, timezone, timedelta

N = 100_000_000  # Number of lines
base = datetime(2025, 9, 1, 12, 0, 0, tzinfo=timezone.utc)

last_print = time.time()

with open("input.json", "w", encoding="utf-8") as f:
    f.write('[')
    for i in range(N):
        obj = {
            "metric": "cpu.util" if i % 3 else "mem.used",
            "value": round(random.random() * 100, 3),
            "ts": (base + timedelta(seconds=i)).isoformat(),
            "node": f"compute-{i % 5}",
            "i": i
        }
        json.dump(obj, f, separators=(',', ':'), ensure_ascii=False)
        if i != N - 1:
            f.write(',')

        # progress log every 5 seconds
        if time.time() - last_print >= 5:
            size = os.path.getsize("input.json") / (1024 * 1024 * 1024)
            print(f"{i+1:,} lines written, file size ~{size:.2f} GB")
            last_print = time.time()

    f.write(']')
print("Done.")
