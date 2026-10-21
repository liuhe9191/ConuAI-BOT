@echo off
chcp 65001 >nul
title ConuAI 核心控制台

echo ====================================
echo      启动 ConuAI 核心控制台...
echo ====================================
echo.

:: 自动打开浏览器访问前端
start http://127.0.0.1:8080

:: 启动 Python 后端
python server.py

:: 如果 Python 异常退出（比如报错、缺少库），则暂停并显示错误
if %errorlevel% neq 0 (
    echo.
    echo [错误] 服务器异常退出！请查看上方的红字报错。
    echo 请检查是否安装了依赖：pip install aiohttp websockets psutil
    pause
)