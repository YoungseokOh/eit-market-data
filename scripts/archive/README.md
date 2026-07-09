# scripts/archive

완료된 일회성 패치/백필/병합 스크립트 보관소. 실행 이력과 방법론 기록을 위해
버전 관리에 남겨두되, 현역 `scripts/` 목록에서는 분리한다.

여기 있는 스크립트는 작성 시점의 데이터 상태를 전제로 하므로 **재실행 전에
전제 조건이 아직 유효한지 반드시 확인**한다. 일부는 `scripts/`의 다른 모듈을
`from build_kr_snapshot import ...`처럼 직접 import하므로, 재실행이 필요하면
repo 루트에서 `PYTHONPATH=scripts` 또는 원래 위치로 되돌린 뒤 실행한다.

| 스크립트 | 용도 (완료 시점) |
|---|---|
| `apply_kr_short_ban.py` | KR 공매도 금지 metadata 스탬프 (2020-03~05, 2023-11~2025-03) |
| `apply_wsl_dns_config.sh` | WSL2 시절 DNS 설정 (현재 macOS 환경에선 불필요) |
| `build_kr_backfill_2019.py` | KOSPI200 2019+ 히스토리컬 백필 드라이버 |
| `fix_seam_adjustment_drift.py` | US CRWD/DD seam 조정 드리프트 수정 |
| `merge_us_sp400.py` | US S&P400 additive 병합 (90/90 완료, 인수 확정) |
| `run_sp400_merge.sh` / `drive_sp400.sh` / `sp400_finalize.sh` / `sp400_watch.sh` | SP400 병합 드라이버/워처 |
| `patch_kr_quality_fields.py` | KR quality/accruals 5필드 추가 패치 |
| `patch_kr_top500_benchmark_gaps.py` | KR top-500 benchmark_prices 19개월 갭 패치 (commit cc2661e) |
| `patch_us_gross_profit.py` | US gross_profit 커버리지 확장 패치 |
| `probe_fdr_krx_session.py` | FDR/KRX 세션 진단 프로브 |
| `rebucket_dart_fundamentals_2019.py` | DART 캐시 2019 앵커 재버킷 (히스토리컬 백필 방법론) |
