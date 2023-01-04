# docker build -t brian/sz_sqs_consumer .
# docker run --user $UID -it -v $PWD:/data -e AWS_DEFAULT_REGION -e AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN -e SENZING_ENGINE_CONFIGURATION_JSON brian/sz_sqs_consumer <queue url>

ARG BASE_IMAGE=senzing/senzingapi-runtime:latest
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2022-08-27

LABEL Name="brain/sz_sqs_consumer" \
      Maintainer="brianmacy@gmail.com" \
      Version="DEV"

RUN apt-get update \
 && apt-get -y install curl python3 python3-pip python3-boto3 python3-psycopg2 \
 && python3 -mpip install orjson \
 && apt-get -y remove build-essential python3-pip \
 && apt-get -y autoremove \
 && apt-get -y clean

COPY sz_sqs_consumer.py /app/
RUN curl -X GET \
      --output /app/senzing_governor.py \
      https://raw.githubusercontent.com/Senzing/governor-postgresql-transaction-id/main/senzing_governor.py

ENV PYTHONPATH=/opt/senzing/g2/sdk/python:/app

USER 1001

WORKDIR /app
ENTRYPOINT ["/app/sz_sqs_consumer.py"]

