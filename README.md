# eit-market-data

Standalone point-in-time market data layer for EIT.

## Setup

```bash
uv sync --extra all --extra dev
```

If you are not using `uv`, install the package in editable mode:

```bash
pip install -e '.[all,dev]'
```

## KR Preflight

After a reboot or WSL2 network change, run:

```bash
python scripts/preflight_kr_data.py --as-of 2026-03-06 --ticker 005930
```

This checks:
- WSL2 detection and `/etc/resolv.conf`
- DNS resolution for KRX/Naver/ECOS
- `pykrx` price, benchmark, and sector lookup
- DART fundamentals
- ECOS macro coverage

## WSL2 Notes

- Apply the known-good DNS config with `scripts/apply_wsl_dns_config.sh`
- Run `wsl --shutdown` from Windows after changing `/etc/wsl.conf`
- `.bashrc` sources `scripts/auto_shell.sh`, which activates `.venv` and loads `.env`

See [docs/wsl2-runbook.md](docs/wsl2-runbook.md) for the full runbook.

## Docs

- [docs/api-keys.md](docs/api-keys.md)
- [docs/eit-research-data-requirements.md](docs/eit-research-data-requirements.md)
- [docs/wsl2-runbook.md](docs/wsl2-runbook.md)
