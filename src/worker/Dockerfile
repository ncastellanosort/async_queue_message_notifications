# stage 1
FROM python:3.11-slim as BUILDER

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# stage 2
FROM python:3.11-slim

WORKDIR /worker

COPY --from=builder /install /usr/local

COPY . .

RUN useradd -m worker
USER worker

CMD ["python3", "worker.py"]
