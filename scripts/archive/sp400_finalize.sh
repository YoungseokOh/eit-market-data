#!/usr/bin/env bash
# Post-merge acceptance for the S&P MidCap 400 additive merge.
#
#   1. Byte-identity check: every pre-existing (large-cap) ticker's serialized
#      prices/fundamentals/filings/sector must be unchanged vs the pre-merge
#      backup (proves no re-adjustment drift).
#   2. Delisted / empty-price mid-cap manifest.
#   3. (optional) forensics is run separately from eit-research.
#
# Run from repo root after the merge reports finished.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
set -a; [ -f ./.env ] && . ./.env; set +a

echo "=== byte-identity check (existing large-cap tickers unchanged) ==="
python - <<'PY'
import json,glob,hashlib
from pathlib import Path
baks=sorted(glob.glob("artifacts/us/snapshots.bak-pre-sp400-*"))
if not baks:
    print("NO pre-sp400 backup found — cannot verify"); raise SystemExit(1)
bak=baks[-1]
def entry(snap,t):
    return hashlib.sha256(json.dumps({
        "p":snap["prices"].get(t),"f":snap["fundamentals"].get(t),
        "fl":snap["filings"].get(t),"s":snap["sector_map"].get(t)},
        sort_keys=True).encode()).hexdigest()
checked=changed=0
for p in sorted(glob.glob("artifacts/us/snapshots/*/snapshot.json")):
    m=Path(p).parent.name
    op=Path(bak)/m/"snapshot.json"
    if not op.exists(): continue
    new=json.loads(Path(p).read_text()); old=json.loads(op.read_text())
    for t in old["universe"]:
        checked+=1
        if entry(old,t)!=entry(new,t):
            changed+=1; print("CHANGED",m,t)
print(f"byte-identity: checked={checked} changed={changed}")
print("RESULT: PASS" if changed==0 else "RESULT: FAIL")
PY

echo
echo "=== delisted / empty-price mid-cap manifest ==="
python scripts/build_us_sp400_delisted_manifest.py

echo
echo "=== universe size distribution ==="
python - <<'PY'
import json,glob
from pathlib import Path
sizes=[]
for p in sorted(glob.glob("artifacts/us/snapshots/*/snapshot.json")):
    d=json.loads(Path(p).read_text())
    priced=sum(1 for t,v in d["prices"].items() if v)
    sizes.append((Path(p).parent.name,len(d["universe"]),priced))
print(f"months={len(sizes)} universe min/max={min(s[1] for s in sizes)}/{max(s[1] for s in sizes)} "
      f"avg={sum(s[1] for s in sizes)//len(sizes)}")
print(f"priced coverage avg={sum(s[2] for s in sizes)//len(sizes)}")
for name,u,pr in sizes[:2]+sizes[-2:]:
    print(f"  {name}: universe={u} priced={pr}")
PY
echo
echo "Forensics: run from eit-research:"
echo "  cd /Users/ysoh/Projects/eit-research && python scripts/backfill_forensics.py --market us --universe-type broad"
