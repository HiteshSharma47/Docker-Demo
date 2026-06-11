from python:slim

WORKDIR /python-demo
COPY demo.py .

CMD ["python","demo.py"]