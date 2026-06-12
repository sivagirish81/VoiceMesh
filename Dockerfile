FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY apps/__init__.py ./apps/__init__.py
RUN pip install --upgrade pip && pip install .
COPY apps/api ./apps/api
COPY apps/worker ./apps/worker
COPY scripts ./scripts

CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
