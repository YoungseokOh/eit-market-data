@echo off
setlocal
set SCRIPT_DIR=%~dp0
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%windows_krx_setup_and_probe.ps1" %*
exit /b %ERRORLEVEL%
