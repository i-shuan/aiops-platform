# 第一階段：把 React 管理介面編譯成靜態檔（需要連得到 npm registry；離線環境請先在有網路的機器 build 好映像檔再匯入）
FROM node:22-slim AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/index.html ui/vite.config.js ./
COPY ui/src ./src
RUN npm run build

# 第二階段：服務本體。映像檔裡沒有 Node，只有編譯好的靜態檔（由 src/admin.py 在 /ui/ 底下提供）
FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY --from=ui /ui/dist ./ui/dist
USER 10001
EXPOSE 8080
CMD ["python", "-m", "src.server"]
