FROM python:3

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 59000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "59000"]
