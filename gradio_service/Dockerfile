# Базовый образ с Python и slim-версией Debian
FROM python:3.10-slim

WORKDIR /gradio_service

# Установка Node.js и npm + dbml-renderer
RUN apt-get update && apt-get install -y curl \
 && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
 && apt-get install -y nodejs \
 && npm install -g @softwaretechnik/dbml-renderer \
 && npm install -g @dbml/cli \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Установка Python-зависимостей
COPY req2.txt .
RUN pip install --no-cache-dir -r req2.txt

# Копируем приложение
COPY . .

# Создаём непривилегированного пользователя и переключаемся
RUN useradd -m pptruser \
 && chown -R pptruser:pptruser /gradio_service
USER pptruser

CMD ["python", "-u", "app.py"]

