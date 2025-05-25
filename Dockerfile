FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (including Google Cloud)
RUN pip install --no-cache-dir -r requirements.txt

# Download ML models (one-time setup)
RUN python -c "\
import os; \
os.environ['TRANSFORMERS_CACHE'] = '/app/.cache'; \
from transformers import AutoTokenizer, AutoModelForSequenceClassification; \
print('📥 Downloading DistilBERT tokenizer...'); \
tokenizer = AutoTokenizer.from_pretrained('distilbert-base-uncased'); \
print('📥 Downloading DistilBERT model...'); \
model = AutoModelForSequenceClassification.from_pretrained('distilbert-base-uncased'); \
print('✅ Models downloaded and cached successfully'); \
"

# Copy application code
COPY . .

# Set environment variables
ENV TRANSFORMERS_CACHE=/app/.cache
ENV PYTHONPATH=/app

# Command is specified in docker-compose.yml
