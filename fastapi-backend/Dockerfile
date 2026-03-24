# Use UV Docker image with Python 3.12 Trixie
FROM ghcr.io/astral-sh/uv:python3.12-trixie

# Set working directory
WORKDIR /app

# Copy pyproject.toml and uv.lock
COPY pyproject.toml uv.lock ./

# Sync dependencies using uv
RUN uv sync

# Copy the application code
COPY . .

# Make start.sh executable
RUN chmod +x start.sh

# Expose port for FastAPI
EXPOSE 8000

# Expose port for Debugpy
EXPOSE 5678

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Use start.sh script
ENTRYPOINT ["bash", "start.sh"]

