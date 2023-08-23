# Health Auto Export Ingestion

Based on the code from [ivailop7/Healthkit-influxdb-grafana](https://github.com/ivailop7/Healthkit-influxdb-grafana).

Takes config in the form of environment variables listed below 

```shell
# Optional: Useful for inspection of data and latter collation if something goes wrong.
# Will only store data if this is set 
export DATA_STORE=./json-store

# Influx communication config
export INFLUX_HOST=localhost
export INFLUX_PORT=8086
export INFLUXDB_DB=health
```

Exposes a health endpoint at `/health`.
