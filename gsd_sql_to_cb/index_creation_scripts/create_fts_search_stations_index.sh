#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 server"
  exit 1
fi
#server="adb-cb4.gsd.esrl.noaa.gov"
#server="localhost"

curl -XPUT -H "Content-Type: application/json" \
-u <username>:<password> http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo -d \
'{
  "type": "fulltext-index",
  "name": "station_geo",
  "uuid": "1cb8cdc388e13dad",
  "sourceType": "couchbase",
  "sourceName": "mdata",
  "sourceUUID": "096a42f61beb578dfc5d1e3286e98876",
  "planParams": {
    "maxPartitionsPerPIndex": 86,
    "indexPartitions": 12,
    "numReplicas": 2
  },
  "params": {
    "doc_config": {
      "docid_prefix_delim": "",
      "docid_regexp": "^DD:V03:METAR:station",
      "mode": "docid_regexp",
      "type_field": "type"
    },
    "mapping": {
      "analysis": {},
      "default_analyzer": "standard",
      "default_datetime_parser": "dateTimeOptional",
      "default_field": "_all",
      "default_mapping": {
        "dynamic": true,
        "enabled": false
      },
      "default_type": "_default",
      "docvalues_dynamic": true,
      "index_dynamic": true,
      "store_dynamic": false,
      "type_field": "_type",
      "types": {
        "DD:V03:METAR:station": {
          "dynamic": false,
          "enabled": true,
          "properties": {
            "description": {
              "dynamic": false,
              "enabled": true,
              "fields": [
                {
                  "docvalues": true,
                  "include_in_all": true,
                  "include_term_vectors": true,
                  "index": true,
                  "name": "description",
                  "store": true,
                  "type": "text"
                }
              ]
            },
            "geo": {
              "dynamic": false,
              "enabled": true,
              "fields": [
                {
                  "docvalues": true,
                  "include_in_all": true,
                  "include_term_vectors": true,
                  "index": true,
                  "name": "geo",
                  "store": true,
                  "type": "geopoint"
                }
              ]
            },
            "name": {
              "dynamic": false,
              "enabled": true,
              "fields": [
                {
                  "docvalues": true,
                  "include_in_all": true,
                  "include_term_vectors": true,
                  "index": true,
                  "name": "name",
                  "store": true,
                  "type": "text"
                }
              ]
            }
          }
        }
      }
    },
    "store": {
      "indexType": "scorch"
    }
  },
  "sourceParams": {}
}'