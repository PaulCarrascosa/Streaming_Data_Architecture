#!/bin/bash

# Get environment
ENV=${ENVIRONMENT:-development}

# Check if production
if [ "$ENV" = "production" ]; then
    echo "🚀 Starting in PRODUCTION mode with Gunicorn (4 workers)..."
    uv run gunicorn main:app \
        --config gunicorn.conf.py \
        --bind 0.0.0.0:8000 \
        --workers 4
else
    echo "🐛 Starting in DEVELOPMENT mode with Debugpy (1 worker)..."
    uv run python3.13 -m debugpy \
        --listen 0.0.0.0:5678 \
        -m gunicorn main:app \
        --bind 0.0.0.0:8000 \
        --workers 1 \
        --reload
fi
