#!/usr/bin/env python3
import argparse, json, subprocess, sys, time
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Force re-submit chunks starting at seq n (manifest-only).")
    ap.add_argument("out_dir", help="Directory with manifest.json and chunk files")
    ap.add_argument("--start-at", type=int, required=True, help="First seq to submit (inclusive)")
    ap.add_argument("--endpoint", required=True, help="https URL to /submit/ndjson")
    ap.add_argument("--bearer", required=True, help="Bearer token")
    ap.add_argument("--idem-key", default=None, help="Override Idempotency-Key (default: manifest's)")
    ap.add_argument("--limit", type=int, default=None, help="Max number of chunks to send")
    ap.add_argument("--continue-on-error", action="store_true", help="Don’t stop on first failure")
    ap.add_argument("--verbose", action="store_true", help="Show curl output")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        sys.exit(f"manifest.json not found in {out_dir}")

    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    idem = args.idem_key or m.get("idempotency_key")
    if not idem:
        sys.exit("No idempotency_key in manifest; pass --idem-key")

    chunks = sorted((m.get("chunks") or []), key=lambda c: int(c["seq"]))
    chunks = [c for c in chunks if int(c["seq"]) >= args.start_at]
    if args.limit is not None:
        chunks = chunks[:args.limit]

    print(f"[plan] submitting {len(chunks)} chunk(s) from seq={args.start_at}")
    sent = 0
    for c in chunks:
        seq = int(c["seq"])
        path = c["path"]
        gzip = bool(c.get("gzip"))
        headers = [
            "-H", f"Authorization: Bearer {args.bearer}",
            "-H", "Accept: application/json",
            "-H", "Content-Type: application/x-ndjson",
            "-H", f"Idempotency-Key: {idem}",
            "-H", f"X-Batch-Seq: {seq}",
        ]
        if gzip:
            headers += ["-H", "Content-Encoding: gzip"]

        cmd = [
            "curl", "--fail-with-body", "-sS", "-X", "POST", *headers,
            "--data-binary", f"@{path}",
            "-w", "\nHTTP_STATUS=%{http_code}\n",
            args.endpoint,
        ]
        print(f"[upload] seq={seq} file={path}")
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        ok = False
        body_lines = []
        for line in p.stdout:
            if args.verbose:
                print(line.rstrip())
            body_lines.append(line.rstrip())
            if line.startswith("HTTP_STATUS=2"):
                ok = True
        rc = p.wait()
        if not ok or rc != 0:
            print(f"[ERROR] seq={seq} failed (rc={rc})", file=sys.stderr)
            # show last few lines to understand the error
            print("\n".join(body_lines[-10:]), file=sys.stderr)
            if not args.continue_on_error:
                sys.exit(1)
        else:
            sent += 1
            time.sleep(0.02)  # tiny pacing to be gentle
    print(f"[done] sent={sent} chunks (idem={idem})")

if __name__ == "__main__":
    main()
