FROM mapsanalytics.azurecr.io/databricks:9.1

RUN apt-get update \
    && apt-get install -y sudo

WORKDIR /app
COPY . /app
RUN pip install -r /app/requirements.txt

ENTRYPOINT ["bash", "entrypoint.sh"]
