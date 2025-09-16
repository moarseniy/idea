# Базовый образ с Python и slim‑версией Debian
FROM python:3.10-slim

# Установка всего сразу, чтобы не плодить слои
RUN apt-get update && apt-get install -y \
      curl \
      ca-certificates \
      git \
      build-essential \
      libnss3 \
      libxss1 \
      libasound2 \
      libxtst6 \
      libgbm1 \
      libgtk-3-0 \
      libgdk-pixbuf2.0-0 \
      libgl1 \
      fonts-freefont-ttf \
      fontconfig \
      chromium \
    && rm -rf /var/lib/apt/lists/*

# Node.js 18 + глобальная установка bpmn-to-image
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
 && apt-get update && apt-get install -y nodejs \
 && npm install -g bpmn-to-image \
 && npm install -g bpmnlint \
 && npm cache clean --force \
 && rm -rf /var/lib/apt/lists/*

# Говорим Puppeteer, где хром и что запускать без песочницы
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    PUPPETEER_ARGS="--no-sandbox --disable-setuid-sandbox" \
    DANGEROUSLY_DISABLE_SANDBOX=1

WORKDIR /gradio_service

# Установка Python‑зависимостей
COPY req2.txt .
RUN pip install --no-cache-dir -r req2.txt

RUN bpmnlint --init

# Копируем приложение
COPY . .

RUN cd bpmn-auto-layout && npm install -g .

# Создаём непривилегированного пользователя и переключаемся
RUN useradd -m pptruser \
 && chown -R pptruser:pptruser /gradio_service
USER pptruser

CMD ["python", "app.py"]
