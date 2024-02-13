ede_kafka_detection = {
    "schema": {
        "type": "struct",
        "fields": [
            {
                "type": "int64",
                "optional": "false",
                "field": "cycle_start"
            },
            {
                "type": "int64",
                "optional": "false",
                "field": "cycle_end"
            },
            {
                "type": "int8",
                "optional": "false",
                "field": "cycle_type"
            },
            {
                "type": "string",
                "optional": "false",
                "field": "sid"
            },
            {
                "type": "string",
                "optional": "false",
                "field": "node"
            },
            {
                "type": "string",
                "optional": "false",
                "field": "cluster"
            },
            {
                "type": "string",
                "optional": "false",
                "field": "anomaly"
            }
        ]
    },
    "payload": {}
}