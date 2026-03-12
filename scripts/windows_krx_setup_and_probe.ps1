param(
    [int]$Timeout = 600,
    [string]$Ticker = "005930",
    [string]$Start = "2026-03-01",
    [string]$End = "2026-03-12",
    [string]$Market = "KRX",
    [switch]$SkipLogin,
    [switch]$ForceLogin
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Get-BasePython {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return @("py", "-3")
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @("python")
    }
    throw "Python launcher not found. Install Python 3.11+ and ensure 'py' or 'python' is on PATH."
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Command
    )

    $Exe = $Command[0]
    $Args = @()
    if ($Command.Length -gt 1) {
        $Args = $Command[1..($Command.Length - 1)]
    }

    Write-Host ">> $($Command -join ' ')" -ForegroundColor Cyan
    & $Exe @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $($Command -join ' ')"
    }
}

$BasePython = Get-BasePython
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvPlaywright = Join-Path $RepoRoot ".venv\Scripts\playwright.exe"

if (-not (Test-Path $VenvPython)) {
    Invoke-Checked ($BasePython + @("-m", "venv", ".venv"))
}

Invoke-Checked @($VenvPython, "-m", "pip", "install", "--upgrade", "pip")
Invoke-Checked @($VenvPython, "-m", "pip", "install", "-e", ".[kr]")
Invoke-Checked @($VenvPlaywright, "install", "chromium")

if (-not $SkipLogin) {
    $LoginArgs = @($VenvPython, "scripts/krx_login.py", "--timeout", "$Timeout")
    if ($ForceLogin) {
        $LoginArgs += "--force"
    }
    Invoke-Checked $LoginArgs
}

Invoke-Checked @(
    $VenvPython,
    "scripts/probe_fdr_krx_session.py",
    "--ticker",
    $Ticker,
    "--start",
    $Start,
    "--end",
    $End,
    "--market",
    $Market
)

$CookiePath = & $VenvPython -c "from eit_market_data.kr.krx_auth import resolve_cookie_path; print(resolve_cookie_path())"

Write-Host ""
Write-Host "KRX cookies:" -ForegroundColor Green
Write-Host $CookiePath.Trim()
