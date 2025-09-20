# Базовый образ с Python и slim‑версией Debian
FROM python:3.10-slim

WORKDIR /gradio_service

# Установка Python‑зависимостей
COPY req2.txt .
RUN pip install --no-cache-dir -r req2.txt

# Копируем приложение
COPY . .

# Создаём непривилегированного пользователя и переключаемся
RUN useradd -m pptruser \
 && chown -R pptruser:pptruser /gradio_service
USER pptruser

CMD ["python", "app.py"]
