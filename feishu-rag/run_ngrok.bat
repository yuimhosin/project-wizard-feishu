@echo off
chcp 65001 >nul
echo ========================================
echo  ngrok 隧道 - 飞书事件订阅
echo ========================================
echo.
echo 请确保 run_bot.py 已在另一终端运行（端口 9000）
echo.
ngrok http 9000
pause
