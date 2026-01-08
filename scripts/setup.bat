@echo off
REM ctOS - One-Time Server Setup Script
REM Run this once before first deployment

echo =========================================
echo ctOS Server Setup
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

REM Step 1: Create directory structure
echo Step 1: Creating directory structure...
if not exist "C:\ctos" mkdir "C:\ctos"
if not exist "C:\ctos\data" mkdir "C:\ctos\data"
if not exist "C:\ctos\logs" mkdir "C:\ctos\logs"
if not exist "C:\ctos\ssl" mkdir "C:\ctos\ssl"
echo   Directories created
echo.

REM Step 2: Check for Docker
echo Step 2: Checking Docker installation...
docker --version >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: Docker not found
    echo Please install Docker Desktop from https://docker.com
    pause
    exit /b 1
)
echo   Docker found
echo.

REM Step 3: Generate SSL certificates
echo Step 3: Generating SSL certificates...
cd C:\ctos\ssl

REM Check if OpenSSL is available (comes with Git for Windows)
where openssl >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: OpenSSL not found
    echo Install Git for Windows or add OpenSSL to PATH
    pause
    exit /b 1
)

REM Generate certificate for ctos-server
echo   Generating certificate for ctos-server...
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/C=US/ST=State/L=City/O=Company/CN=ctos-server" -addext "subjectAltName=DNS:ctos-server,DNS:ctos-demo,IP:127.0.0.1"

if exist "cert.pem" (
    echo   SSL certificates created successfully
) else (
    echo ERROR: Certificate generation failed
    pause
    exit /b 1
)
echo.

REM Step 4: Clone repository (if not already done)
echo Step 4: Repository setup...
if not exist "C:\ctos\repo" (
    echo   Cloning repository...
    set /p REPO_URL="Enter Git repository URL: "
    cd C:\ctos
    git clone %REPO_URL% repo
    cd repo
    git checkout production
) else (
    echo   Repository already exists
    cd C:\ctos\repo
    git checkout production
    git pull origin production
)
echo.

REM Step 5: Environment file setup
echo Step 5: Environment file setup...
if not exist "C:\ctos\repo\.env" (
    echo   Creating .env file from template...
    copy "C:\ctos\repo\.env.example" "C:\ctos\repo\.env"
    echo.
    echo   IMPORTANT: Edit C:\ctos\repo\.env and add your OPENAI_API_KEY
    echo   Press any key after editing .env file...
    pause
) else (
    echo   .env file already exists
)
echo.

REM Step 6: Verify products.db exists
echo Step 6: Checking production database...
if not exist "C:\ctos\data\products.db" (
    echo WARNING: products.db not found in C:\ctos\data\
    echo   Please copy your production database to C:\ctos\data\products.db
    echo   Press any key after copying database...
    pause
)
echo.

REM Step 7: Generate demo seed database
echo Step 7: Generating demo seed database...
cd C:\ctos\repo
python scripts\create_demo_data.py
if %errorLevel% neq 0 (
    echo WARNING: Demo data generation failed
    echo   This is not critical. You can run create_demo_data.py manually later.
) else (
    echo   Demo seed database created successfully
)
echo.

REM Step 8: Build Docker images
echo Step 8: Building Docker images...
cd C:\ctos\repo
docker-compose build
if %errorLevel% neq 0 (
    echo ERROR: Docker build failed
    pause
    exit /b 1
)
echo   Docker images built successfully
echo.

REM Step 9: Configure hosts file
echo Step 9: Configuring hosts file...
echo   Adding hostname entries to hosts file...
findstr /C:"ctos-server" C:\Windows\System32\drivers\etc\hosts >nul
if %errorLevel% neq 0 (
    echo 127.0.0.1    ctos-server >> C:\Windows\System32\drivers\etc\hosts
    echo 127.0.0.1    ctos-demo >> C:\Windows\System32\drivers\etc\hosts
    echo   Hostname entries added
) else (
    echo   Hostname entries already exist
)
echo.

REM Final validation
echo =========================================
echo Setup Validation
echo =========================================
echo.

echo Checking setup completion:
if exist "C:\ctos\data\products.db" (echo   [OK] Production database) else (echo   [MISSING] Production database)
if exist "C:\ctos\data\demo_seed.db" (echo   [OK] Demo seed database) else (echo   [MISSING] Demo seed database)
if exist "C:\ctos\ssl\cert.pem" (echo   [OK] SSL certificate) else (echo   [MISSING] SSL certificate)
if exist "C:\ctos\repo\.env" (echo   [OK] Environment file) else (echo   [MISSING] Environment file)
docker images | findstr ctos >nul 2>&1 && (echo   [OK] Docker images) || (echo   [MISSING] Docker images)
echo.

echo =========================================
echo Setup Complete
echo =========================================
echo.
echo Next steps:
echo   1. Verify .env file has OPENAI_API_KEY set
echo   2. Run deploy.bat to start production
echo   3. Access at https://ctos-server:8501
echo.
pause