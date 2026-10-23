FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 happybase==1.2.0
COPY writer.py .
CMD ["python", "-u", "writer.py"]
