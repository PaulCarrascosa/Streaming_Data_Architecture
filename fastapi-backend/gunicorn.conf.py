# Gunicorn configuration file for FastAPI with uvicorn workers
import multiprocessing

# Server socket configuration
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes - Reduced for stability
workers = 4  # Reduced from (cpu_count * 2 + 1) for better resource management
worker_class = "uvicorn_worker.UvicornWorker"
worker_connections = 1000
timeout = 120

# Timeouts
keepalive = 5
graceful_timeout = 30

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "fastapi-streaming"

# Reload
reload = False

# Server mechanics
daemon = False
umask = 0
