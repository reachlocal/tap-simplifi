import json

streams = ['campaign_conversion_summary_reports', 'campaign_general_summary_reports', 'ad_summary_reports', 'ad_network_publisher_reports',
    'ad_geofence_reports', 'campaign_keyword_reports', 'campaign_geofence_reports', 'ad_device_reports', 'ad_conversion_reports',
    'campaign_network_publisher_reports', 'campaign_device_reports', 'ad_keyword_reports']


field_map = {
    "integer": "INT64",
    "string": "STRING",
    "number": "FLOAT64",
    "boolean": "BOOL"
}

for stream in streams:
    table = f'simplifi.{stream}'
    schema = {}
    with open(f'tap_simplifi/schemas/{stream}.json') as f:
        schema = json.load(f)

    props = []
    for attr, value in schema["properties"].items():
        if value["label"] == "Time Event Date":
            props.append(f'{attr} DATE')
        else:
            props.append(f'{attr} {field_map[value["type"]]}')

    query = f'CREATE TABLE {table} ({",".join(props)}) PARTITION BY event_date;\n'
    print(query)
