"""Classify S&P MidCap 400 names that carry empty price arrays (delisted /
removed / never-recovered) by exit reason, and quantify the residual coverage
hole honestly.

Mirrors ``scripts/build_us_delisted_manifest.py`` (large-cap) but scoped to the
mid-cap names the S&P 400 merge added. Mid-caps delist/purge more often than
large-caps and free price recovery (yfinance + stockanalysis.com fallback) is
partial, so this must NOT silently ship a survivorship-biased set: it reports
the fraction of mid-cap name-months with no price, and flags any known
failure-to-zero (bankruptcy) whose collapse path is missing from the data.

Categories (same as the large-cap manifest):
  - ma             : acquired / merged (cash or stock; omitting tail is conservative)
  - spinoff        : split / reorganization (basis change, not a zero)
  - still_trading  : renamed / index-removed but still listed (empty = data gap)
  - failure_to_zero: bankruptcy / receivership (omission INFLATES returns — FLAG)
  - unclassified   : empty-price mid-cap not yet hand-classified (the honest hole)

Emits docs/us_sp400_delisted_manifest.{md,json}.
"""

from __future__ import annotations

import csv
import glob
import json
from collections import Counter
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_US = _REPO_ROOT / "artifacts" / "us" / "snapshots"
_SP400_CSV = _REPO_ROOT / "universes" / "us" / "sp400"

# Hand-maintained classification for mid-cap exits. Seeded with known cases;
# extend as needed. Names NOT here that carry empty prices are reported as
# "unclassified" (the honest residual hole), so the manifest never pretends to
# be complete.
CLASSIFY: dict[str, tuple[str, str]] = {
    # --- failure-to-zero (bankruptcy) — FLAG ---
    "BBBY": ("Bed Bath & Beyond — Chapter 11 2023-04; shareholders wiped", "failure_to_zero"),
    "SIX": ("Six Flags — merged w/ Cedar Fair 2024 (survived as FUN); n/a", "ma"),
    "WEX": ("still trading", "still_trading"),
    # --- acquired / merged ---
    "ALEX": ("Alexander & Baldwin — index rebalance; still trades (REIT)", "still_trading"),
    "PDCE": ("PDC Energy acquired by Chevron 2023-08", "ma"),
    "CONE": ("CyrusOne taken private by KKR/GIP 2021-11", "ma"),
    "QTS": ("QTS Realty taken private by Blackstone 2021-08", "ma"),
    "CDK": ("CDK Global taken private by Brookfield 2022-07", "ma"),
    "NUAN": ("Nuance acquired by Microsoft 2022-03", "ma"),
    "COHR": ("Coherent acquired by II-VI 2022-07 (survived as COHR)", "ma"),
    "SGEN": ("Seagen acquired by Pfizer 2023-12", "ma"),
    "MANT": ("ManTech taken private by Carlyle 2022-09", "ma"),
    "CATM": ("Cardtronics acquired by NCR 2021-06", "ma"),
}


def _sp400_origin_tickers() -> set[str]:
    """Union of all tickers that appeared in any monthly S&P 400 CSV."""
    out: set[str] = set()
    for p in sorted(glob.glob(str(_SP400_CSV / "*.csv"))):
        with open(p, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                t = str(row.get("ticker", "")).strip()
                if t:
                    out.add(t)
    return out


def main() -> int:
    snaps: dict[str, dict] = {}
    for p in sorted(glob.glob(str(_US / "*" / "snapshot.json"))):
        m = Path(p).parent.name
        snaps[m] = json.loads(Path(p).read_text())
    if not snaps:
        print("no snapshots found — run the merge first")
        return 1
    months = sorted(snaps)

    sp400_names = _sp400_origin_tickers()
    # Large-cap baseline = names present in the pre-sp400 backup universes, so we
    # only attribute residual holes to genuinely mid-cap-origin names.
    baseline: set[str] = set()
    bak = sorted(glob.glob(str(_US.parent / "snapshots.bak-pre-sp400-*")))
    if bak:
        for p in glob.glob(str(Path(bak[-1]) / "*" / "snapshot.json")):
            baseline |= set(json.loads(Path(p).read_text()).get("universe", []))
    midcap_only = sp400_names - baseline

    # Per-name coverage across the months it is a universe member.
    rows = []
    total_name_months = 0
    empty_name_months = 0
    for t in sorted(midcap_only):
        in_months = [m for m in months if t in set(snaps[m]["universe"])]
        if not in_months:
            continue
        priced = [m for m in in_months if (snaps[m].get("prices", {}).get(t) or [])]
        empty = [m for m in in_months if not (snaps[m].get("prices", {}).get(t) or [])]
        total_name_months += len(in_months)
        empty_name_months += len(empty)
        # A name "exits" if it is a member in some month then absent in a later month.
        last_member = months.index(in_months[-1])
        exited = last_member < len(months) - 1
        reason, cat = CLASSIFY.get(
            t,
            ("empty-price mid-cap not hand-classified" if empty else "fully priced", ""),
        )
        if not cat:
            cat = "priced" if not empty else "unclassified"
        last_close = snaps[priced[-1]]["prices"][t][-1]["close"] if priced else None
        rows.append({
            "ticker": t,
            "category": cat,
            "reason": reason,
            "universe_span": f"{in_months[0]}..{in_months[-1]}",
            "months_in_universe": len(in_months),
            "months_priced": len(priced),
            "months_empty": len(empty),
            "exited_index": exited,
            "any_prices": bool(priced),
            "last_priced_month": priced[-1] if priced else None,
            "last_close": last_close,
        })

    counts = Counter(r["category"] for r in rows)
    # Residual hole = names that are members but never recover any price.
    never_priced = [r for r in rows if not r["any_prices"]]
    ftz = [r for r in rows if r["category"] == "failure_to_zero"]
    ftz_no_path = [r for r in ftz if not r["any_prices"]]

    coverage = 1.0 - (empty_name_months / total_name_months) if total_name_months else 0.0

    out = {
        "midcap_origin_names": len(midcap_only),
        "names_ever_member": len(rows),
        "names_never_priced": len(never_priced),
        "name_month_coverage": round(coverage, 4),
        "total_name_months": total_name_months,
        "empty_name_months": empty_name_months,
        "counts": dict(counts),
        "failure_to_zero_no_collapse_path": [r["ticker"] for r in ftz_no_path],
        "tickers": rows,
    }
    (_REPO_ROOT / "docs" / "us_sp400_delisted_manifest.json").write_text(
        json.dumps(out, indent=2)
    )

    lines = [
        "# S&P MidCap 400 delisted / empty-price manifest",
        "",
        f"Mid-cap-origin names (in any monthly S&P 400 CSV, excluding the large-cap "
        f"baseline): **{len(midcap_only)}**; ever a universe member: **{len(rows)}**.",
        "",
        f"**Name-month price coverage: {coverage:.1%}** "
        f"({total_name_months - empty_name_months}/{total_name_months} member "
        f"name-months priced; {empty_name_months} empty).",
        "",
        f"Names that NEVER recover a price (pure survivorship hole): "
        f"**{len(never_priced)}**.",
        "",
        "Category counts: " + ", ".join(f"{k}={v}" for k, v in counts.most_common()),
        "",
        "## FAILURE-TO-ZERO (omission INFLATES returns — consumer must handle)",
        "",
        "| ticker | span | any prices | last priced | reason |",
        "|---|---|---|---|---|",
    ]
    for r in ftz:
        lines.append(
            f"| **{r['ticker']}** | {r['universe_span']} | {r['any_prices']} | "
            f"{r['last_priced_month'] or '—'} ({r['last_close'] or '—'}) | {r['reason']} |"
        )
    if not ftz:
        lines.append("| — | — | — | — | none hand-classified yet |")
    lines += [
        "",
        f"Failure-to-zero names with NO collapse path in the data: "
        f"{[r['ticker'] for r in ftz_no_path] or 'none'}. These must be force-exited "
        "by the consumer at their bankruptcy date or dropped; free sources do not "
        "carry the wipeout path.",
        "",
        "## Unclassified empty-price mid-caps (the honest residual hole)",
        "",
        "These are member name-months with no recoverable price and no hand "
        "classification yet. Their bias direction is unknown until classified; a "
        "conservative consumer should treat unresolved empties as delistings and "
        "not assume survival.",
        "",
        "| ticker | span | months empty/total | exited index |",
        "|---|---|---|---|",
    ]
    for r in rows:
        if r["category"] == "unclassified":
            lines.append(
                f"| {r['ticker']} | {r['universe_span']} | "
                f"{r['months_empty']}/{r['months_in_universe']} | {r['exited_index']} |"
            )
    lines += [
        "",
        "## Classified exits",
        "",
        "| ticker | category | reason |",
        "|---|---|---|",
    ]
    for r in rows:
        if r["category"] in ("ma", "spinoff", "still_trading", "failure_to_zero"):
            lines.append(f"| {r['ticker']} | {r['category']} | {r['reason']} |")
    lines += [
        "",
        "## Honesty note",
        "",
        f"- Mid-cap free price recovery is partial: {coverage:.1%} of member "
        "name-months are priced. The unpriced remainder is a survivorship gap; it "
        "is documented above rather than hidden by dropping the names from the "
        "universe (names stay in `universe` with empty `prices`, same pattern as "
        "the large-cap set).",
        "- Closing the remaining hole fully requires a paid source (CRSP/Sharadar) "
        "for delisted mid-cap price paths, which is out of scope.",
    ]
    (_REPO_ROOT / "docs" / "us_sp400_delisted_manifest.md").write_text("\n".join(lines) + "\n")

    print(
        f"midcap names ever member={len(rows)} coverage={coverage:.1%} "
        f"never_priced={len(never_priced)} counts={dict(counts)}"
    )
    print(f"FAILURE-TO-ZERO no-path: {[r['ticker'] for r in ftz_no_path]}")
    print("wrote docs/us_sp400_delisted_manifest.{md,json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
