import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os

INFLUXDB_TOKEN="4kF2LkIAl_8Z6EnyF31Qd4paxQeDaTayCs0-onBQVlt5V1jWu82KSRg6AI0mwbh8-91S-C-z7ztqLSFIFochsQ=="

# token = os.environ.get("INFLUXDB_TOKEN")
org = "primary"
url = "https://us-west-2-1.aws.cloud2.influxdata.com/"

client = influxdb_client.InfluxDBClient(url=url, token=INFLUXDB_TOKEN, org=org)

query_api = client.query_api()
query = """from(bucket: "primary")
 |> range(start: -10m)
 |> filter(fn: (r) => r._measurement == "measurement1")"""
tables = query_api.query(query, org="primary")

for table in tables:
    for record in table.records:
        print(record)