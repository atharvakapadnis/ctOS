@echo off
REM ctOS - Production Deployment Script
REM Deploys latest code from production branch

echo =========================================
echo ctOS Production Deployment
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

REM Step 1: Pre-deployment checks
echo Step 1: Pre-deployment checks...
git rev-parse --abbrev-ref HEAD > temp_branch.txt
set /p CURRENT_BRANCH=<temp_branch.txt
del temp_branch.txt

if not "%CURRENT_BRANCH%"=="production" (
    echo ERROR: Not on production branch (currently on %CURRENT_BRANCH%)
    echo Run: git checkout production
    pause
    exit /b 1
)
echo   On production branch
echo.

REM Step 2: Git pull latest
echo Step 2: Pulling latest code...
git fetch origin production
git pull origin production
if %errorLevel% neq 0 (
    echo ERROR: Git pull failed
    echo Check for merge conflicts
    pause
    exit /b 1
)

REM Get current commit hash for logging
git rev-parse --short HEAD > temp_commit.txt
set /p COMMIT_HASH=<temp_commit.txt
del temp_commit.txt
echo   Latest commit: %COMMIT_HASH%
echo.

REM Step 3: Optional tagging
echo Step 3: Version tagging...
set /p TAG_RELEASE="Tag this release? (y/n): "
if /i "%TAG_RELEASE%"=="y" (
    set /p VERSION_TAG="Enter version tag (e.g., v1.0.0): "
    git tag -a %VERSION_TAG% -m "Production release %VERSION_TAG%"
    git push origin %VERSION_TAG%
    echo   Tagged as %VERSION_TAG%
) else (
    echo   Skipping tagging
)
echo.

REM Step 4: Build new Docker image
echo Step 4: Building Docker image...
docker-compose build ctos-prod
if %errorLevel% neq 0 (
    echo ERROR: Docker build failed
    pause
    exit /b 1
)

REM Tag with commit hash
docker tag ctos-repo-ctos-prod:latest ctos:commit-%COMMIT_HASH%
if defined VERSION_TAG (
    docker tag ctos-repo-ctos-prod:latest ctos:%VERSION_TAG%
)
echo   Image built and tagged
echo.

REM Step 5: Check if container is running
echo Step 5: Checking current deployment...
docker ps | findstr ctos-prod >nul 2>&1
if %errorLevel% equ 0 (
    set CONTAINER_RUNNING=true
    echo   ctos-prod container is currently running
) else (
    set CONTAINER_RUNNING=false
    echo   No running ctos-prod container found
)
echo.

REM Step 6: Stop old container
if "%CONTAINER_RUNNING%"=="true" (
    echo Step 6: Stopping old container...
    docker-compose stop ctos-prod
    echo   Container stopped (not removed yet for quick rollback)
    echo.
) else (
    echo Step 6: No container to stop
    echo.
)

REM Step 7: Start new container
echo Step 7: Starting new container...
docker-compose up -d ctos-prod
if %errorLevel% neq 0 (
    echo ERROR: Container startup failed
    echo Attempting automatic rollback...
    call scripts\rollback.bat auto
    pause
    exit /b 1
)
echo   Container started
echo.

REM Step 8: Health check
echo Step 8: Health check...
echo   Waiting 15 seconds for startup...
timeout /t 15 /nobreak >nul

docker ps | findstr ctos-prod >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: Container not running after startup
    echo Attempting automatic rollback...
    call scripts\rollback.bat auto
    pause
    exit /b 1
)

echo   Checking container logs...
docker-compose logs ctos-prod --tail=20 > temp_logs.txt
findstr /C:"error" /C:"ERROR" /C:"Exception" temp_logs.txt >nul 2>&1
if %errorLevel% equ 0 (
    echo WARNING: Errors detected in logs
    type temp_logs.txt
    echo.
    set /p CONTINUE="Continue despite errors? (y/n): "
    if /i not "%CONTINUE%"=="y" (
        echo Attempting automatic rollback...
        del temp_logs.txt
        call scripts\rollback.bat auto
        pause
        exit /b 1
    )
)
del temp_logs.txt
echo   Health check passed
echo.

REM Step 9: Remove old container
if "%CONTAINER_RUNNING%"=="true" (
    echo Step 9: Cleaning up old container...
    docker-compose rm -f ctos-prod 2>nul
    echo   Old container removed
    echo.
) else (
    echo Step 9: No cleanup needed
    echo.
)

REM Step 10: Log deployment
echo Step 10: Logging deployment...
echo %DATE% %TIME% - Deployed commit %COMMIT_HASH% >> C:\ctos\logs\deployments.log
if defined VERSION_TAG (
    echo %DATE% %TIME% - Version: %VERSION_TAG% >> C:\ctos\logs\deployments.log
)
echo   Deployment logged
echo.

REM Display summary
echo =========================================
echo Deployment Complete
echo =========================================
echo.
echo Commit: %COMMIT_HASH%
if defined VERSION_TAG (
    echo Version: %VERSION_TAG%
)
echo Container: ctos-prod
echo Access URL: https://ctos-server:8501
echo.
echo Container status:
docker ps | findstr ctos-prod
echo.
echo Recent logs:
docker-compose logs ctos-prod --tail=10
echo.
pause