FROM python:3.8

WORKDIR /python_etl

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY source/ source/
COPY input/ input/

ENV PYTHONPATH=/python_etl

CMD ["python", "source/etl.py"]