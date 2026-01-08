#!/bin/bash
set -e

echo "========================"
echo "ctOS Container Starting"
echo "========================"

# Get APP_MODE from environment (default to prod)
APP_MODE=${APP_MODE:-prod}

echo "Mode: $APP_MODE"
echo "Timestamp: $(date)"

# Demo mode: Reset database before starting
if [ "$APP_MODE" = "demo" ]; then
    echo "========================"
    echo "Demo Mode: Resetting Database"
    echo "========================"

    DEMO_SEED="/app/data/demo_seed.db"
    DEMO_DB="/app/data/demo_products.db"

    # Check if demo seed exists
    if [ ! -f "$DEMO_SEED" ]; then
        echo "ERROR: Demo seed database not found at $DEMO_SEED"
        echo "Please run create_demo_data.py first."
        exit 1
    fi

    # Copy seed to working database
    echo "Copying $DEMO_SEED to $DEMO_DB"
    cp "$DEMO_SEED" "$DEMO_DB"

    # Set permissions 
    chmod 644 "$DEMO_DB"

    echo "Demo database reset completed successfully"
    echo "Products reset to unprocessed state"
    echo "========================"
fi

# Start Streamlit
echo "Starting Streamlit application..."
echo "========================"

exec streamlit run streamlit_app.py \
    --server.port=$STREAMLIT_SERVER_PORT \
    --server.address=$STREAMLIT_SERVER_ADDRESS \
    --server.enable_cors=$STREAMLIT_SERVER_ENABLE_CORS \
    --server.enable_xsrf_protection=$STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION \
    --server.sslCertFile=$STREAMLIT_SERVER_SSL_CERT_FILE \
    --server.sslKeyFile=$STREAMLIT_SERVER_SSL_KEY_FILE