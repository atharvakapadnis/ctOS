@echo off
REM ctOS - Production Rollback Script
REM Reverts to previous version

setlocal enabledelayedexpansion

echo =========================================
echo ctOS Production Rollback
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

REM Check for auto mode (called from deploy.bat)
set AUTO_MODE=%1

REM Navigate to repository
cd C:\ctos\repo
if %errorLevel% neq 0 (
    echo ERROR: Repository not found at C:\ctos\repo
    pause
    exit /b 1
)

REM Step 1: List available versions
if not "%AUTO_MODE%"=="auto" (
    echo Step 1: Available versions...
    echo.
    echo Docker Images:
    echo --------------
    docker images ctos --format "table {{.Tag}}\t{{.CreatedAt}}\t{{.Size}}"
    echo.
    echo Git Tags:
    echo ---------
    git tag -l --sort=-creatordate | head -10
    echo.
    
    REM Step 2: Prompt for version
    echo Step 2: Select rollback target...
    set /p ROLLBACK_VERSION="Enter version tag or 'latest' for previous build: "
    
    if "%ROLLBACK_VERSION%"=="" (
        echo ERROR: No version specified
        pause
        exit /b 1
    )
    
    REM Step 3: Confirmation
    echo.
    echo WARNING: This will rollback production to %ROLLBACK_VERSION%
    set /p CONFIRM="Continue with rollback? (yes/no): "
    if /i not "%CONFIRM%"=="yes" (
        echo Rollback cancelled
        pause
        exit /b 0
    )
    echo.
) else (
    echo AUTO ROLLBACK MODE
    echo Attempting to restore previous container...
    echo.
    set ROLLBACK_VERSION=auto
)

REM Step 4: Stop current container
echo Step 4: Stopping current container...
docker-compose stop ctos-prod
docker-compose rm -f ctos-prod
echo   Container stopped and removed
echo.

REM Step 5: Rollback method
if "%ROLLBACK_VERSION%"=="auto" (
    echo Step 5: Automatic rollback - using previous Docker image...
    REM Find second most recent image (first is failed deployment)
    docker images ctos --format "{{.Tag}}" | findstr /v "latest" > temp_tags.txt
    set /p PREV_TAG=<temp_tags.txt
    del temp_tags.txt
    
    if "%PREV_TAG%"=="" (
        echo ERROR: No previous image found for rollback
        echo Manual intervention required
        pause
        exit /b 1
    )
    
    echo   Rolling back to: ctos:!PREV_TAG!
    
    docker run -d ^
        --name ctos-prod ^
        -p 8501:8501 ^
        -v C:\ctos\data\products.db:/app/data/products.db ^
        -v C:\ctos\logs:/app/logs ^
        -v C:\ctos\repo\.env:/app/.env:ro ^
        -v C:\ctos\ssl:/app/ssl:ro ^
        -e APP_MODE=prod ^
        -e STREAMLIT_SERVER_PORT=8501 ^
        -e STREAMLIT_SERVER_ADDRESS=0.0.0.0 ^
        -e STREAMLIT_SERVER_ENABLE_CORS=false ^
        -e STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true ^
        -e STREAMLIT_SERVER_SSL_CERT_FILE=/app/ssl/cert.pem ^
        -e STREAMLIT_SERVER_SSL_KEY_FILE=/app/ssl/key.pem ^
        --restart always ^
        ctos:!PREV_TAG!
        
) else if "%ROLLBACK_VERSION%"=="latest" (
    echo Step 5: Docker-based rollback - using latest image...
    docker-compose up -d ctos-prod
) else (
    echo Step 5: Version-specific rollback...
    
    REM Check if Docker image exists
    docker images ctos:%ROLLBACK_VERSION% | findstr %ROLLBACK_VERSION% >nul 2>&1
    if %errorLevel% equ 0 (
        echo   Using Docker image: ctos:%ROLLBACK_VERSION%
        
        docker run -d ^
            --name ctos-prod ^
            -p 8501:8501 ^
            -v C:\ctos\data\products.db:/app/data/products.db ^
            -v C:\ctos\logs:/app/logs ^
            -v C:\ctos\repo\.env:/app/.env:ro ^
            -v C:\ctos\ssl:/app/ssl:ro ^
            -e APP_MODE=prod ^
            -e STREAMLIT_SERVER_PORT=8501 ^
            -e STREAMLIT_SERVER_ADDRESS=0.0.0.0 ^
            -e STREAMLIT_SERVER_ENABLE_CORS=false ^
            -e STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true ^
            -e STREAMLIT_SERVER_SSL_CERT_FILE=/app/ssl/cert.pem ^
            -e STREAMLIT_SERVER_SSL_KEY_FILE=/app/ssl/key.pem ^
            --restart always ^
            ctos:%ROLLBACK_VERSION%
            
    ) else (
        echo   Docker image not found, checking Git tag...
        git tag -l | findstr %ROLLBACK_VERSION% >nul 2>&1
        if %errorLevel% neq 0 (
            echo ERROR: Version %ROLLBACK_VERSION% not found in Docker images or Git tags
            pause
            exit /b 1
        )
        
        echo   Git-based rollback to %ROLLBACK_VERSION%
        git checkout %ROLLBACK_VERSION%
        docker-compose build ctos-prod
        docker-compose up -d ctos-prod
        git checkout production
    )
)

if %errorLevel% neq 0 (
    echo ERROR: Rollback failed
    pause
    exit /b 1
)
echo   Rollback container started
echo.

REM Step 6: Health check
echo Step 6: Health check...
echo   Waiting 15 seconds for startup...
timeout /t 15 /nobreak >nul

docker ps | findstr ctos-prod >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: Container not running after rollback
    echo Manual intervention required
    docker-compose logs ctos-prod --tail=30
    pause
    exit /b 1
)
echo   Container running
echo.

REM Step 7: Log rollback
echo Step 7: Logging rollback...
if not "%AUTO_MODE%"=="auto" (
    set /p ROLLBACK_REASON="Enter reason for rollback: "
    echo %DATE% %TIME% - ROLLBACK to %ROLLBACK_VERSION% - Reason: !ROLLBACK_REASON! >> C:\ctos\logs\deployments.log
) else (
    echo %DATE% %TIME% - AUTO ROLLBACK - Deployment failure >> C:\ctos\logs\deployments.log
)
echo   Rollback logged
echo.

REM Display summary
echo =========================================
echo Rollback Complete
echo =========================================
echo.
if not "%ROLLBACK_VERSION%"=="auto" (
    echo Rolled back to: %ROLLBACK_VERSION%
) else (
    echo Auto-rollback to previous version
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
if not "%AUTO_MODE%"=="auto" pause