@echo off
REM ctOS - Demo Container Deployment Script
REM Starts demo environment for presentations

echo =========================================
echo ctOS Demo Environment Startup
echo =========================================
echo.

REM Check if running as administrator
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This script requires Administrator privileges
    echo Right-click and select "Run as administrator"
    pause
    exit /b 1
)

REM Navigate to repository
cd C:\ctos\repo
if %errorLevel% neq 0 (
    echo ERROR: Repository not found at C:\ctos\repo
    echo Run setup.bat first
    pause
    exit /b 1
)

REM Step 1: Verify demo seed database exists
echo Step 1: Verifying demo seed database...
if not exist "C:\ctos\data\demo_seed.db" (
    echo ERROR: Demo seed database not found
    echo Run: python scripts\create_demo_data.py
    pause
    exit /b 1
)
echo   Demo seed database found
echo.

REM Step 2: Stop existing demo container
echo Step 2: Stopping existing demo container (if running)...
docker ps | findstr ctos-demo >nul 2>&1
if %errorLevel% equ 0 (
    docker-compose stop ctos-demo
    docker-compose rm -f ctos-demo
    echo   Existing demo container stopped and removed
) else (
    echo   No existing demo container found
)
echo.

REM Step 3: Build demo image if needed
echo Step 3: Checking Docker image...
docker images | findstr ctos-repo-ctos-demo >nul 2>&1
if %errorLevel% neq 0 (
    echo   Building demo image...
    docker-compose build ctos-demo
    if %errorLevel% neq 0 (
        echo ERROR: Docker build failed
        pause
        exit /b 1
    )
) else (
    echo   Demo image exists
)
echo.

REM Step 4: Start demo container
echo Step 4: Starting demo container...
docker-compose up -d ctos-demo
if %errorLevel% neq 0 (
    echo ERROR: Demo container startup failed
    pause
    exit /b 1
)
echo   Demo container started
echo.

REM Step 5: Health check
echo Step 5: Health check...
echo   Waiting 15 seconds for startup and data reset...
timeout /t 15 /nobreak >nul

docker ps | findstr ctos-demo >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: Demo container not running after startup
    echo Checking logs...
    docker-compose logs ctos-demo --tail=30
    pause
    exit /b 1
)

echo   Checking demo reset completion...
docker-compose logs ctos-demo | findstr /C:"Demo database reset completed" >nul 2>&1
if %errorLevel% neq 0 (
    echo WARNING: Demo reset confirmation not found in logs
    echo Check logs below:
    docker-compose logs ctos-demo --tail=20
    echo.
    set /p CONTINUE="Continue anyway? (y/n): "
    if /i not "%CONTINUE%"=="y" (
        docker-compose stop ctos-demo
        pause
        exit /b 1
    )
) else (
    echo   Demo data reset successful
)
echo.

REM Display summary
echo =========================================
echo Demo Environment Ready
echo =========================================
echo.
echo Demo URL: https://ctos-demo:8502
echo Demo database: Fresh 250 unprocessed products
echo All processing data: Reset to initial state
echo.
echo Container status:
docker ps | findstr ctos-demo
echo.
echo Recent logs:
docker-compose logs ctos-demo --tail=10
echo.
echo To stop demo after presentation:
echo   docker-compose stop ctos-demo
echo.
pause