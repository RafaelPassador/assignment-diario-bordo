FROM bitnami/spark:3.5.1

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/app"

CMD ["python"]