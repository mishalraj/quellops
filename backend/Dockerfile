FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY backend/src /app/backend/src
COPY backend/service_map.yaml /app/backend/service_map.yaml
ENV AIDBG_SERVICE_MAP_PATH=/app/backend/service_map.yaml
ENV PYTHONPATH=/app/backend/src
ENV AIDBG_OTLP_GRPC_BIND=0.0.0.0:4317
ENV AIDBG_HTTP_BIND=0.0.0.0:8088
EXPOSE 4317 8088
CMD ["python", "-m", "aidbg.main"]
