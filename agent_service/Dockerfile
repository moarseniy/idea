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

WORKDIR /agent_service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN bpmnlint --init

COPY . .

RUN apt-get update && apt-get install -y git-lfs
RUN git lfs install
# RUN git clone https://huggingface.co/t-tech/T-pro-it-1.0 /agent_service/llm

# Создаём непривилегированного пользователя и переключаемся
RUN useradd -m pptruser \
 && chown -R pptruser:pptruser /agent_service
USER pptruser

CMD ["python", "app.py"]
