@echo off
chcp 65001 >nul
title MobileWork 对话跟踪观察台
cd /d "%~dp0"
"runtime\node.exe" launcher.mjs
if errorlevel 1 (
  echo.
  echo 启动失败。请截图此窗口并联系维护人员。
  pause
)
