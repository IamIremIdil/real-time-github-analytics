FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY dashboard.py .

EXPOSE 8501

HEALTHCHECK CMD curl -f http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]