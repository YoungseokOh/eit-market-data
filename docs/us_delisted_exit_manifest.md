# US delisted / empty-price universe members — exit-reason manifest

Total classified: 56  |  ma=41, still_trading=9, spinoff=3, failure_to_zero=3

## FAILURE-TO-ZERO (bias INFLATES returns — consumer must handle)

| ticker | universe span | any prices | last priced | reason |
|---|---|---|---|---|
| **FRC** | 2019-01..2023-04 | False | — (—) | First Republic — FDIC receivership, sold to JPMorgan 2023-05; wiped |
| **SBNY** | 2021-12..2023-02 | False | — (—) | Signature Bank — FDIC receivership 2023-03; shareholders wiped |
| **SIVB** | 2019-01..2023-02 | True | 2022-12 (230.14) | SVB Financial — FDIC receivership 2023-03; shareholders wiped |

All three are 2023 bank receiverships. Their collapse paths are NOT in the
data: SIVB's series stops 2022-12 at 230.14 (pre-collapse), SBNY and FRC have
no price bars at all. A backtest holding them through the failure would miss
the loss and overstate returns — the consumer should force-exit these at their
receivership date (SIVB 2023-03-10, SBNY 2023-03-12, FRC 2023-05-01) or drop them.

## Acquired / merged (omitting the tail is CONSERVATIVE)

| ticker | reason |
|---|---|
| ABMD | Abiomed acquired by Johnson & Johnson 2022-12 |
| AGN | Allergan acquired by AbbVie 2020-05 |
| ALXN | Alexion acquired by AstraZeneca 2021-07 |
| APC | Anadarko acquired by Occidental 2019-08 |
| ATVI | Activision Blizzard acquired by Microsoft 2023-10 |
| CELG | Celgene acquired by Bristol-Myers Squibb 2019-11 |
| CERN | Cerner acquired by Oracle 2022-06 |
| CTLT | Catalent acquired by Novo Holdings 2024 |
| CTXS | Citrix taken private by Vista/Elliott 2022-09 |
| CXO | Concho Resources acquired by ConocoPhillips 2021-01 |
| DFS | Discover acquired by Capital One 2025 |
| DISCA | Discovery merged into Warner Bros. Discovery 2022-04 |
| DISCK | Discovery merged into Warner Bros. Discovery 2022-04 |
| DISH | DISH Network merged into EchoStar 2023-12 (stock-for-stock) |
| DRE | Duke Realty acquired by Prologis 2022-10 |
| ETFC | E*Trade acquired by Morgan Stanley 2020-10 |
| FLIR | FLIR Systems acquired by Teledyne 2021-05 |
| HES | Hess acquired by Chevron (pending); still listed in-window |
| HFC | HollyFrontier merged into HF Sinclair 2022 |
| INFO | IHS Markit acquired by S&P Global 2022-02 |
| JNPR | Juniper acquired by HPE 2025 |
| K | Kellogg split (Kellanova) then Mars acquisition 2025 |
| KSU | Kansas City Southern acquired by Canadian Pacific 2021-12 |
| LLL | L3 Technologies merged into L3Harris 2019-06 |
| MRO | Marathon Oil acquired by ConocoPhillips 2024-11 |
| MXIM | Maxim Integrated acquired by Analog Devices 2021-08 |
| NBL | Noble Energy acquired by Chevron 2020-10 |
| NLSN | Nielsen taken private by PE consortium 2022-10 |
| PBCT | People's United acquired by M&T Bank 2022-04 |
| PXD | Pioneer Natural Resources acquired by ExxonMobil 2024-05 |
| RHT | Red Hat acquired by IBM 2019-07 |
| RTN | Raytheon merged into Raytheon Technologies (RTX) 2020-04 |
| STI | SunTrust merged into Truist (with BB&T) 2019-12 |
| TIF | Tiffany & Co. acquired by LVMH 2021-01 |
| TSS | Total System Services acquired by Global Payments 2019-09 |
| VAR | Varian acquired by Siemens Healthineers 2021-04 |
| VIAB | Viacom merged with CBS into ViacomCBS 2019-12 |
| WBA | Walgreens taken private by Sycamore 2025 |
| WCG | WellCare acquired by Centene 2020-01 |
| XEC | Cimarex merged into Coterra Energy 2021-10 |
| XLNX | Xilinx acquired by AMD 2022-02 |

## Spinoff / reorganization

| ticker | reason |
|---|---|
| ARNC | Arconic Inc split into Howmet (HWM) + Arconic Corp 2020-04 |
| DWDP | DowDuPont split into DOW / DD / CTVA 2019 |
| FBHS | Fortune Brands split (Fortune Brands Innovations) 2022 |

## Renamed / index-removed but still trading (empty array = data gap, not an exit)

| ticker | reason |
|---|---|
| ADS | Alliance Data renamed Bread Financial (BFH); still trades |
| ANSS | Ansys index removal; later Synopsys acq 2025; traded in-window |
| CMA | Comerica index removal; still trades |
| DAY | Ceridian renamed Dayforce (DAY); still trades |
| GPS | Gap index removal; still trades (GAP) |
| HOLX | Hologic index removal; still trades |
| IPG | Interpublic index removal; Omnicom merger 2025; traded in-window |
| RE | Everest Re renamed Everest Group (EG); still trades |
| SEE | Sealed Air index removal; still trades |

## Bias summary
- 41 M&A + 3 spinoff: conservative (tail omission understates returns).
- 9 renamed/still-trading: data gaps, not real exits; no path-to-zero.
- 3 failure-to-zero (SIVB/SBNY/FRC): the only names whose omission
  INFLATES returns; flagged above with receivership dates. Closing them fully needs a paid
  source (CRSP/Sharadar); until then the consumer must force-exit them at receivership.
