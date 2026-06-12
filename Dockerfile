FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY apps ./apps
COPY scripts ./scripts
RUN pip install --upgrade pip && pip install .

CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

