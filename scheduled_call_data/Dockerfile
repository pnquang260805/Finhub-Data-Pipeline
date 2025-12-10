FROM python:3.13.9-slim

RUN apt-get update -y && apt-get install cron dos2unix -y && systemctl enable cron

WORKDIR /call_data
COPY . .
RUN pip install -r ./requirements.txt
RUN touch /call_data/cron.log
COPY ./crontab /etc/cron.d/mycron

RUN chmod 0644 /etc/cron.d/mycron
RUN dos2unix /etc/cron.d/mycron
RUN crontab /etc/cron.d/mycron
CMD ["sh", "-c", "echo 'Starting cron service...' && cron && tail -f /call_data/cron.log"]
