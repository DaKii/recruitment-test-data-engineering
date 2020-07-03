FROM python:3.8

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip freeze
COPY example.py ./
ENTRYPOINT ./example.py


