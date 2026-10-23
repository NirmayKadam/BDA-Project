FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit==1.39.0 pandas==2.2.3 kafka-python==2.0.2 requests==2.32.3 happybase==1.2.0
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false"]
