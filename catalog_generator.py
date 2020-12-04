import simplejson as json

selected = ["AD_PERFORMANCE_REPORT", "AGE_RANGE_PERFORMANCE_REPORT",
    "CAMPAIGN_PERFORMANCE_REPORT", "GENDER_PERFORMANCE_REPORT", "KEYWORDS_PERFORMANCE_REPORT",
    "PARENTAL_STATUS_PERFORMANCE_REPORT", "PLACEHOLDER_FEED_ITEM_REPORT", "PRODUCT_PARTITION_REPORT",
    "SEARCH_QUERY_PERFORMANCE_REPORT", "SHOPPING_PERFORMANCE_REPORT", "STATS_BY_DEVICE_AND_NETWORK_REPORT",
    "STATS_BY_DEVICE_HOURLY_REPORT", "STATS_IMPRESSIONS_REPORT", "STATS_WITH_SEARCH_IMPRESSIONS_REPORT",
	"VIDEO_CAMPAIGN_PERFORMANCE_REPORT", "VIDEO_PERFORMANCE_REPORT"]

catalog = {
    "streams": []
}

for stream in selected:
    catalog["streams"].append({
        "tap_stream_id": stream,
			"stream": stream,
			"schema": {
				"type": [
					"null",
					"object"
				],
				"additionalProperties": False
			},
			"key_properties": [],
            "metadata": [
				{
					"breadcrumb": [],
					"metadata": {
						"selected": True
					}
                }
            ]
    })

print(json.dumps(catalog, indent=4))