# WSL2 Runbook

Known-good post-reboot setup for this repository.

## 1. DNS configuration

If DNS resolution breaks after a reboot, apply the fixed resolver config:

```bash
sudo bash scripts/apply_wsl_dns_config.sh
```

This writes:

```ini
[boot]
systemd=true

[user]
default=seok436

[network]
generateResolvConf=false
```

and pins `/etc/resolv.conf` to:

```text
nameserver 1.1.1.1
nameserver 8.8.8.8
```

After changing `/etc/wsl.conf`, run this from Windows:

```powershell
wsl --shutdown
```

Then reopen the distro.

## 2. Shell bootstrap

`/home/seok436/.bashrc` sources `scripts/auto_shell.sh`.

When you enter `/home/seok436/projects/eit-market-data`, it will:

- run `uv sync --extra all --extra dev` when the venv is missing or stale
- activate `.venv`
- load `.env`
- if `/mnt/c/Users/$USER/.cache/eit-market-data/krx-profile/cookies.json` exists,
  export `EIT_KRX_COOKIE_PATH` and `EIT_KRX_PROFILE_DIR` to reuse the Windows KRX session

Do not commit `cookies.json` into the repository. It is a live authenticated session artifact.

## 3. Post-reboot verification

Run the repo preflight:

```bash
python scripts/preflight_kr_data.py --as-of 2026-03-06 --ticker 005930
```

Expected outcome:

- DNS checks for `data.krx.co.kr`, `fchart.stock.naver.com`, `ecos.bok.or.kr` are `OK`
- `pykrx:prices` and `pykrx:benchmark` are `OK`
- `dart` is `OK`
- `ecos` is `OK` or explicitly `DEGRADED`

Exit codes:

- `0`: all checks passed
- `1`: at least one hard failure
- `2`: no hard failures, but at least one degraded dependency

## 4. Current caveats

- `pykrx` index ticker metadata is unstable upstream, so index OHLCV falls back to Yahoo data when needed.
- Live sector classifications may be unavailable upstream. Runtime uses the latest cached sector snapshot on or before `as_of`.
- ECOS monthly and quarterly series must be fetched with verified item codes and full-page pagination; this repository now handles that internally.
