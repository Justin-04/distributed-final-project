FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# CPU-only PyTorch first (~200 MB). From PyPI, Linux gets the CUDA build (~2.5 GB with
# NVIDIA libraries), which these containers never use; installing it here first means
# requirements.txt (torch>=2.0) is already satisfied and the CUDA build is never pulled.
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY services/agent-system-a/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/agent-system-a/ .

COPY models/bert_intent_classifier/model/ /app/models/bert_intent_classifier/model/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
