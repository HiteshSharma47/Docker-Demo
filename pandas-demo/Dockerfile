FROM python:slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY pandas-demo.py .

CMD ["python", "pandas-demo.py"]