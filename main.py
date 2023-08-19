import json
import os
import sys
import logging
from datetime import datetime
from itertools import groupby

from flask import request, Flask
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

app = Flask(__name__)
app.debug = True

data_store = os.environ.get("DATA_STORE", None)
influx_host = os.environ.get("INFLUX_HOST", "influx")
influx_port = os.environ.get("INFLUX_PORT", 8086)

client = InfluxDBClient(host=influx_host, port=influx_port)
client.create_database("db")
client.switch_database("db")


def field_or_tag(datapoint: dict, field: str) -> str:
    if type(datapoint[field]) in [int, float]:
        return "field"
    else:
        return "tag"


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
            metric_fields = set(datapoint.keys())
            metric_fields.remove("date")

            metric_fields = dict(groupby(metric_fields, field_or_tag))
            number_fields = metric_fields["field"]
            string_fields = metric_fields["tag"]

            point = {
                "measurement": metric["name"],
                "time": datapoint["date"],
                "tags": {str(field): str(datapoint[field]) for field in string_fields},
                "fields": {
                    str(field): float(datapoint[field]) for field in number_fields
                },
            }

            transformed_data.append(point)

    logger.info(f"Data Transformation Complete")
    logger.info(f"Number of data points to write: {len(transformed_data)}")

    write_to_influx(transformed_data)


@app.route("/collect", methods=["POST", "GET"])
def collect():
    logger.info(f"Request received")

    try:
        if data_store is not None:
            with open(
                os.path.join(data_store, datetime.now().isoformat() + ".json"), "w"
            ) as f:
                f.write(json.dumps(request.data))

        healthkit_data = json.loads(request.data)
    except:
        return "Invalid JSON Received", 400

    try:
        ingest_metrics(healthkit_data.get("data", {}).get("metrics", []))
        ingest_workouts(healthkit_data.get("data", {}).get("workouts", []))
    except:
        logger.exception("Caught Exception. See stacktrace for details.")
        return "Server Error", 500

    return "Success", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
