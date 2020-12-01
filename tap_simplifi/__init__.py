#!/usr/bin/env python3
import os
import simplejson as json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime
from contextlib import closing
import csv


REQUIRED_CONFIG_KEYS = []
LOGGER = singer.get_logger()
base_url = "https://app.simpli.fi/api/organizations"


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        schema = utils.load_json(get_abs_path("schemas/{}.json".format(stream.tap_stream_id.lower())))
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        headers = {
            "X-App-Key": config['appKey'],
            "X-User-Key": config['userKey'],
            "MccUsername": config['username'],
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        retrieve_stats = (stream.tap_stream_id == "ad_reports" or stream.tap_stream_id == "campaign_reports")

        if retrieve_stats:
            data = stats_data(stream, config, headers)
            for row in data:
                singer.write_record(stream.tap_stream_id, json.loads(json.dumps(row)))
        else:
            data = reporting_data(stream, config, headers, schema)

        singer.write_state({"last_updated_at": datetime.now().isoformat(), "stream": stream.tap_stream_id})
    return

def stats_data(stream, config, headers):
    ad_report = stream.tap_stream_id == "ad_reports"
    request_url = f'{base_url}/{config["organizationId"]}/campaign_stats?by_campaign=true&by_ad={ad_report}&start_date={config["startDate"]}&end_date={config["endDate"]}'
    resp = requests.get(request_url, headers = headers)
    data = resp.json()['campaign_stats']
    for row in data:
        del row['resources']

    return data

def reporting_data(stream, config, headers, schema):
    report_map = {
        "campaign_general_summary_reports": { "id": 55990, "date_param": "fact_delivery.event_date"},
        "campaign_conversion_summary_reports": { "id": 55984, "date_param": "summary_delivery_events.event_date"},
        "campaign_geofence_reports": { "id": 55987, "date_param": "summary_delivery_events.event_date"},
        "campaign_device_reports": { "id": 55991, "date_param": "summary_delivery_events.event_date"},
        "campaign_keyword_reports": { "id": 55988, "date_param": "summary_delivery_events.event_date"},
        "campaign_network_publisher_reports": { "id": 55989, "date_param": "summary_delivery_events.event_date"},
        "ad_summary_reports": { "id": 70870, "date_param": "fact_delivery.event_date"},
        "ad_conversion_reports": { "id": 70826, "date_param": "summary_delivery_events.event_date"},
        "ad_device_reports": { "id": 70840, "date_param": "summary_delivery_events.event_date"},
        "ad_keyword_reports": { "id": 70867, "date_param": "summary_delivery_events.event_date"},
        "ad_geofence_reports": { "id": 70863, "date_param": "summary_delivery_events.event_date"},
        "ad_network_publisher_reports": { "id": 70847, "date_param": "summary_delivery_events.event_date"}
    }
    report_id = report_map[stream.tap_stream_id]["id"]
    date_param = report_map[stream.tap_stream_id]["date_param"]
    client_id = 25;

    report_url = f'{base_url}/{client_id}/report_center/reports/{report_id}'
    create_snapshot_url = f'{report_url}/schedules/create_snapshot'

    snapshot_body = {
        "destination_format": "csv",
        "filters": {}
    }
    snapshot_body["filters"][date_param] = config["dateRange"]

    snapshot_created = requests.post(create_snapshot_url, data = json.dumps(snapshot_body), headers = headers).json()
    snapshot_url = f'{report_url}/schedules/snapshots/{snapshot_created["snapshots"][0]["id"]}'

    LOGGER.info(f'Snapshot created: {snapshot_url}')

    report_download_url = ""
    while True:
        snapshot = requests.get(snapshot_url, headers = headers).json()
        if snapshot["snapshots"][0]["status"] == "success":
            report_download_url = snapshot["snapshots"][0]["download_link"]
            break

    LOGGER.info(f'Downloading report: {report_download_url}')

    props = [(k, v) for k, v in schema["properties"].items()]
    with closing(requests.get(report_download_url, stream=True)) as r:
        f = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.reader(f, delimiter=',', quotechar='"')
        
        header = {}
        for row_number, row in enumerate(reader):
            if row_number == 0:
                for idx in range(len(row)):
                    header[row[idx]] = idx
            else:
                mapped = {}
                for i in range(len(props)):
                    try:
                        value = row[header[props[i][1]["label"]]]
                        if props[i][1]["type"] == "number":
                            value = float(value) if "." in value else int(value)
                        mapped[props[i][0]] = value
                    except Exception as ex:
                        LOGGER.info(ex)
                        LOGGER.info(row_number)
                        LOGGER.info(row)
                        LOGGER.info(i)
                
                singer.write_record(stream.tap_stream_id, json.loads(json.dumps(mapped)))

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
