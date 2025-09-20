FROM python:3.10-slim

WORKDIR /agent_service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# RUN git clone https://huggingface.co/t-tech/T-pro-it-1.0 /agent_service/llm

# Создаём непривилегированного пользователя и переключаемся
RUN useradd -m pptruser \
 && chown -R pptruser:pptruser /agent_service
USER pptruser

CMD ["python", "app.py"]
