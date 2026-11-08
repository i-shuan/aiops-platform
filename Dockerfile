# aiops-platform：同一個映像檔跑 API（預設）與排程（python -m aiops.interfaces.cli ...）
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY aiops ./aiops

# 非 root 執行；/data 給沒接 PostgreSQL 時的 SQLite 用
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin aiops \
    && mkdir -p /data && chown 10001 /data
USER 10001

ENV AIOPS_DATABASE_URL=sqlite:////data/aiops.db
EXPOSE 8000

CMD ["uvicorn", "aiops.interfaces.app:app", "--host", "0.0.0.0", "--port", "8000"]
