# prepbufr ingest to couchbase

## purpose

These programs are intended to import prepbufr data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.

## Approach

These programs use a JOB document to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the JOB document.

The ingest_document_ids: ['MD:V01:RAOB:obs:ingest:netcdf'] line defines
a list of metadata documents (might be just one). These documents define how the program will operate.
The 'MD:V01:RAOBS:obs:ingest:netcdf' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host in an associated credentials file (the name of which is provided as a command line parameter) and MUST be readable by the cb_user.

## stations

The station documents in the RAOB collection are RAOB stations.

example

```json
{
    "RAOB": {
      "description": "JAN MAYEN(NOR-NAVY)",
      "docType": "station",
      "geo": [
        {
          "elev": 9,
          "firstTime": 0,
          "lastTime": 1714068000,
          "lat": 70.93,
          "lon": -8.67
        }
      ],
      "gps_date": "",
      "id": "MD:V01:RAOB:station:1001",
      "name": "ENJA",
      "near_airport": "",
      "subset": "RAOB",
      "type": "MD",
      "updateTime": "2024-04-25 12:00:00",
      "version": "V01",
      "wmoid": 1001
    }
  },
  ```

## Builder class

The builder is [PrepbufrBuilder]()

There is a base PrepbufrBuilder which has the generic code for reading a prepbufr file and a specialized PrepbufrRaobsObsBuilderV01 class which knows how to build RAOBS from a prepbufr ADPUPA file.


## Region list

I'm only putting this here temporarily so that I don't lose it before it gets implemented.
RUC domain
RRFS North American domain
Great Lakes
Global (all lat/lon)
Tropics (-20 <= lat <= +20)
Southern Hemisphere (-80 <= lat < -20)
Northern Hemisphere (+20 < lat <= +80)
Arctic (lat >= +70) -- Might want to change this to lat >= 60N to match EMC?
Antarctic (lat <= -70) -- Might want to change this to lat <= 60S to match EMC?
Alaska
Hawaii
HRRR domain
Eastern HRRR domain
Western HRRR domain
CONUS
Eastern CONUS (lon <= 100W)
Western CONUS (lon <= 100W)
Northeastern CONUS
Southeastern CONUS
Central CONUS
Southern CONUS
Northwest CONUS
Southern Plains