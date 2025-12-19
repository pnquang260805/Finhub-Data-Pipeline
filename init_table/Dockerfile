FROM python:3.13.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends nano && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /init_table

COPY . .

RUN pip install -r requirements.txt
CMD [ "python", "./main.py" ]