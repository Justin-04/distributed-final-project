FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# CPU-only PyTorch first (~200 MB) for sentence-transformers (cross-encoder reranker).
# From PyPI, Linux gets the CUDA build (~2.5 GB) which these containers never use.
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

CMD ["uvicorn", "server:http_app", "--host", "0.0.0.0", "--port", "8002"]
