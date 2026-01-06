FROM python:3.10-slim-buster

WORKDIR /app

# Install awscli via pip (NOT apt)
RUN pip install --no-cache-dir awscli

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "app.py"]
