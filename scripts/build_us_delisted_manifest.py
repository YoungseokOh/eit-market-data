"""Classify US universe members that carry empty price arrays (delisted /
removed / never-fetched) by exit reason, flagging failure-to-zero cases.

The 2019-2022 US universe is point-in-time (survivorship-free membership), but
free price sources retroactively purge some removed names, leaving them in the
`universe` list with an empty `prices` array. This manifest classifies each such
name so the consumer knows the bias direction:

  - ma            : acquired / merged (holders got cash or stock at a premium).
                    Omitting the tail is CONSERVATIVE (returns slightly understated).
  - spinoff       : corporate split / reorganization (basis change, not a zero).
  - still_trading : renamed or index-removed but still listed elsewhere; the empty
                    array is a data gap, NOT a real exit — no path-to-zero.
  - failure_to_zero: bankruptcy / receivership; shareholders wiped out. Omitting
                    the collapse INFLATES returns — MUST be handled by the consumer.

Emits docs/us_delisted_exit_manifest.{md,json}.
"""

from __future__ import annotations

import glob
import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_US = _REPO_ROOT / "artifacts" / "us" / "snapshots"

# reason, category. category in {ma, spinoff, still_trading, failure_to_zero}
CLASSIFY: dict[str, tuple[str, str]] = {
    # --- failure-to-zero (bankruptcy / bank receivership) — FLAG ---
    "SIVB": ("SVB Financial — FDIC receivership 2023-03; shareholders wiped", "failure_to_zero"),
    "SBNY": ("Signature Bank — FDIC receivership 2023-03; shareholders wiped", "failure_to_zero"),
    "FRC": ("First Republic — FDIC receivership, sold to JPMorgan 2023-05; wiped", "failure_to_zero"),
    # --- acquired / merged (cash or stock; conservative to omit) ---
    "ABMD": ("Abiomed acquired by Johnson & Johnson 2022-12", "ma"),
    "AGN": ("Allergan acquired by AbbVie 2020-05", "ma"),
    "ALXN": ("Alexion acquired by AstraZeneca 2021-07", "ma"),
    "APC": ("Anadarko acquired by Occidental 2019-08", "ma"),
    "ATVI": ("Activision Blizzard acquired by Microsoft 2023-10", "ma"),
    "CELG": ("Celgene acquired by Bristol-Myers Squibb 2019-11", "ma"),
    "CERN": ("Cerner acquired by Oracle 2022-06", "ma"),
    "CTLT": ("Catalent acquired by Novo Holdings 2024", "ma"),
    "CTXS": ("Citrix taken private by Vista/Elliott 2022-09", "ma"),
    "CXO": ("Concho Resources acquired by ConocoPhillips 2021-01", "ma"),
    "DFS": ("Discover acquired by Capital One 2025", "ma"),
    "DISCA": ("Discovery merged into Warner Bros. Discovery 2022-04", "ma"),
    "DISCK": ("Discovery merged into Warner Bros. Discovery 2022-04", "ma"),
    "DISH": ("DISH Network merged into EchoStar 2023-12 (stock-for-stock)", "ma"),
    "DRE": ("Duke Realty acquired by Prologis 2022-10", "ma"),
    "ETFC": ("E*Trade acquired by Morgan Stanley 2020-10", "ma"),
    "FLIR": ("FLIR Systems acquired by Teledyne 2021-05", "ma"),
    "HES": ("Hess acquired by Chevron (pending); still listed in-window", "ma"),
    "HFC": ("HollyFrontier merged into HF Sinclair 2022", "ma"),
    "INFO": ("IHS Markit acquired by S&P Global 2022-02", "ma"),
    "JNPR": ("Juniper acquired by HPE 2025", "ma"),
    "K": ("Kellogg split (Kellanova) then Mars acquisition 2025", "ma"),
    "KSU": ("Kansas City Southern acquired by Canadian Pacific 2021-12", "ma"),
    "LLL": ("L3 Technologies merged into L3Harris 2019-06", "ma"),
    "MRO": ("Marathon Oil acquired by ConocoPhillips 2024-11", "ma"),
    "MXIM": ("Maxim Integrated acquired by Analog Devices 2021-08", "ma"),
    "NBL": ("Noble Energy acquired by Chevron 2020-10", "ma"),
    "NLSN": ("Nielsen taken private by PE consortium 2022-10", "ma"),
    "PBCT": ("People's United acquired by M&T Bank 2022-04", "ma"),
    "PXD": ("Pioneer Natural Resources acquired by ExxonMobil 2024-05", "ma"),
    "RHT": ("Red Hat acquired by IBM 2019-07", "ma"),
    "RTN": ("Raytheon merged into Raytheon Technologies (RTX) 2020-04", "ma"),
    "STI": ("SunTrust merged into Truist (with BB&T) 2019-12", "ma"),
    "TIF": ("Tiffany & Co. acquired by LVMH 2021-01", "ma"),
    "TSS": ("Total System Services acquired by Global Payments 2019-09", "ma"),
    "VAR": ("Varian acquired by Siemens Healthineers 2021-04", "ma"),
    "VIAB": ("Viacom merged with CBS into ViacomCBS 2019-12", "ma"),
    "WBA": ("Walgreens taken private by Sycamore 2025", "ma"),
    "WCG": ("WellCare acquired by Centene 2020-01", "ma"),
    "XEC": ("Cimarex merged into Coterra Energy 2021-10", "ma"),
    "XLNX": ("Xilinx acquired by AMD 2022-02", "ma"),
    # --- spinoff / reorganization ---
    "DWDP": ("DowDuPont split into DOW / DD / CTVA 2019", "spinoff"),
    "FBHS": ("Fortune Brands split (Fortune Brands Innovations) 2022", "spinoff"),
    "ARNC": ("Arconic Inc split into Howmet (HWM) + Arconic Corp 2020-04", "spinoff"),
    # --- renamed / index-removed but still trading (empty = data gap) ---
    "ADS": ("Alliance Data renamed Bread Financial (BFH); still trades", "still_trading"),
    "ANSS": ("Ansys index removal; later Synopsys acq 2025; traded in-window", "still_trading"),
    "CMA": ("Comerica index removal; still trades", "still_trading"),
    "DAY": ("Ceridian renamed Dayforce (DAY); still trades", "still_trading"),
    "GPS": ("Gap index removal; still trades (GAP)", "still_trading"),
    "HOLX": ("Hologic index removal; still trades", "still_trading"),
    "IPG": ("Interpublic index removal; Omnicom merger 2025; traded in-window", "still_trading"),
    "RE": ("Everest Re renamed Everest Group (EG); still trades", "still_trading"),
    "SEE": ("Sealed Air index removal; still trades", "still_trading"),
}


def main() -> int:
    snaps = {}
    for p in sorted(glob.glob(str(_US / "*" / "snapshot.json"))):
        m = p.split("/")[-2]
        snaps[m] = json.loads(Path(p).read_text())
    months = sorted(snaps)
    final = set(snaps[months[-1]]["universe"])

    rows = []
    for t, (reason, cat) in sorted(CLASSIFY.items()):
        in_months = [m for m in months if t in set(snaps[m]["universe"])]
        priced = [m for m in in_months if (snaps[m].get("prices", {}).get(t) or [])]
        last_close = None
        if priced:
            last_close = snaps[priced[-1]]["prices"][t][-1]["close"]
        rows.append({
            "ticker": t, "category": cat, "reason": reason,
            "universe_span": f"{in_months[0]}..{in_months[-1]}" if in_months else "-",
            "months_in_universe": len(in_months),
            "any_prices": bool(priced),
            "last_priced_month": priced[-1] if priced else None,
            "last_close": last_close,
        })

    ftz = [r for r in rows if r["category"] == "failure_to_zero"]
    from collections import Counter
    counts = Counter(r["category"] for r in rows)

    (_REPO_ROOT / "docs" / "us_delisted_exit_manifest.json").write_text(
        json.dumps({"counts": dict(counts), "tickers": rows}, indent=2)
    )

    lines = [
        "# US delisted / empty-price universe members — exit-reason manifest",
        "",
        f"Total classified: {len(rows)}  |  " + ", ".join(f"{k}={v}" for k, v in counts.most_common()),
        "",
        "## FAILURE-TO-ZERO (bias INFLATES returns — consumer must handle)",
        "",
        "| ticker | universe span | any prices | last priced | reason |",
        "|---|---|---|---|---|",
    ]
    for r in ftz:
        lines.append(
            f"| **{r['ticker']}** | {r['universe_span']} | {r['any_prices']} | "
            f"{r['last_priced_month'] or '—'} ({r['last_close'] or '—'}) | {r['reason']} |"
        )
    lines += [
        "",
        "All three are 2023 bank receiverships. Their collapse paths are NOT in the",
        "data: SIVB's series stops 2022-12 at 230.14 (pre-collapse), SBNY and FRC have",
        "no price bars at all. A backtest holding them through the failure would miss",
        "the loss and overstate returns — the consumer should force-exit these at their",
        "receivership date (SIVB 2023-03-10, SBNY 2023-03-12, FRC 2023-05-01) or drop them.",
        "",
        "## Acquired / merged (omitting the tail is CONSERVATIVE)",
        "",
        "| ticker | reason |",
        "|---|---|",
    ]
    for r in rows:
        if r["category"] == "ma":
            lines.append(f"| {r['ticker']} | {r['reason']} |")
    lines += ["", "## Spinoff / reorganization", "", "| ticker | reason |", "|---|---|"]
    for r in rows:
        if r["category"] == "spinoff":
            lines.append(f"| {r['ticker']} | {r['reason']} |")
    lines += [
        "", "## Renamed / index-removed but still trading (empty array = data gap, not an exit)",
        "", "| ticker | reason |", "|---|---|",
    ]
    for r in rows:
        if r["category"] == "still_trading":
            lines.append(f"| {r['ticker']} | {r['reason']} |")
    lines += [
        "",
        "## Bias summary",
        f"- {counts['ma']} M&A + {counts['spinoff']} spinoff: conservative (tail omission understates returns).",
        f"- {counts['still_trading']} renamed/still-trading: data gaps, not real exits; no path-to-zero.",
        f"- {counts['failure_to_zero']} failure-to-zero (SIVB/SBNY/FRC): the only names whose omission",
        "  INFLATES returns; flagged above with receivership dates. Closing them fully needs a paid",
        "  source (CRSP/Sharadar); until then the consumer must force-exit them at receivership.",
    ]
    (_REPO_ROOT / "docs" / "us_delisted_exit_manifest.md").write_text("\n".join(lines) + "\n")

    print(f"classified {len(rows)}: " + ", ".join(f"{k}={v}" for k, v in counts.most_common()))
    print(f"FAILURE-TO-ZERO: {[r['ticker'] for r in ftz]}")
    print("wrote docs/us_delisted_exit_manifest.{md,json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
