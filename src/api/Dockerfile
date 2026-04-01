# stage 1 -> build
FROM python:3.11-slim AS builder

WORKDIR /builder

# only install system dependencies for python
RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev

COPY requirements.txt .

# save dependencies in /install folder
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# stage 2 -> final version
FROM python:3.11-slim

WORKDIR /app

# copy the files of /install folder (dependencies)
COPY --from=builder /install /usr/local

# app code
COPY . .

EXPOSE 59000

# don't run root on prod
RUN useradd -m api
USER api

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "59000"]
