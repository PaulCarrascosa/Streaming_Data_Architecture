FROM python:3.10

WORKDIR /app

# Installer les dépendances système dont Java 21 (requis par PySpark) et Tesseract
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk netcat-openbsd tesseract-ocr tesseract-ocr-fra && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installer uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copier le code
COPY pyproject.toml uv.lock /app/
COPY wait-for-kafka.sh /app/

# Installer les dépendances via uv (sans groupe dev)
RUN uv sync --no-dev --frozen
COPY app /app/app

RUN chmod +x /app/wait-for-kafka.sh

ENTRYPOINT ["/app/wait-for-kafka.sh"]
