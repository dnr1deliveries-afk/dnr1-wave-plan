FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p uploads
EXPOSE 5001
CMD ["gunicorn", "--config", "gunicorn.conf.py", "app:app"]
