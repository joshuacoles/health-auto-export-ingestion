import json
import os
import sys
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException
from influxdb import InfluxDBClient
from geolib import geohash

DATAPOINTS_CHUNK = 80000

logger = logging.getLogger("console-output")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI()

data_store = os.environ.get("DATA_STORE", None)
influx_host = os.environ.get("INFLUX_HOST", "influx")
influx_port = os.environ.get("INFLUX_PORT", 8086)
influx_db = os.environ.get("INFLUXDB_DB", "health")

client = InfluxDBClient(host=influx_host, port=influx_port)
client.create_database(influx_db)
client.switch_database(influx_db)


def split_fields(datapoint: dict):
    data = {}
    tags = {}

    for field_key in datapoint:
        if field_key in ["date"]:
            continue

        v = datapoint[field_key]
        if type(v) in [int, float]:
            data[field_key] = float(v)
        else:
            tags[field_key] = str(v)

    return data, tags


def write_to_influx(data: list):
    logger.info(f"DB Write Started")

    for i in range(0, len(data), DATAPOINTS_CHUNK):
        logger.info(f"DB Writing chunk")
        client.write_points(data[i : i + DATAPOINTS_CHUNK])

    logger.info(f"DB Metrics Write Complete")


def ingest_workouts(workouts: list):
    logger.info(f"Ingesting Workouts Routes")
    transformed_workout_data = []

    for workout in workouts:
        tags = {"id": workout["name"] + "-" + workout["start"] + "-" + workout["end"]}
        for gps_point in workout["route"]:
            point = {
                "measurement": "workouts",
                "time": gps_point["timestamp"],
                "tags": tags,
                "fields": {
                    "lat": gps_point["lat"],
                    "lng": gps_point["lon"],
                    "geohash": geohash.encode(gps_point["lat"], gps_point["lon"], 7),
                },
            }
            transformed_workout_data.append(point)

        write_to_influx(transformed_workout_data)

    logger.info(f"Ingesting Workouts Complete")


def ingest_metrics(metrics: list):
    logger.info(f"Ingesting Metrics")
    transformed_data = []

    for metric in metrics:
        for datapoint in metric["data"]:
            data, tags = split_fields(datapoint)

            point = {
                "measurement": metric["name"],
                "time": datapoint["date"],
                "tags": tags,
                "fields": data,
            }

            transformed_data.append(point)

    logger.info(f"Data Transformation Complete")
    logger.info(f"Number of data points to write: {len(transformed_data)}")

    write_to_influx(transformed_data)


@app.post("/")
def collect(healthkit_data: dict):
    logger.info(f"Request received")

    if data_store is not None:
        with open(
            os.path.join(data_store, datetime.now().isoformat() + ".json"), "w"
        ) as f:
            f.write(json.dumps(healthkit_data))

    try:
        ingest_metrics(healthkit_data.get("data", {}).get("metrics", []))
        ingest_workouts(healthkit_data.get("data", {}).get("workouts", []))
    except Exception as e:
        logger.exception("Caught Exception. See stacktrace for details.")
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")

    return "Ok"


@app.get("/health")
def health():
    return "Ok"


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("my_package.main:app", host="0.0.0.0", port=7788, reload=True)
