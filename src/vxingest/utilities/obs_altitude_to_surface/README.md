# obs_altitude_to_surface

A Go utility for reprocessing surface pressure in observation documents stored in Couchbase. It reads METAR observation documents, computes station surface pressure from altimeter and elevation, and updates the documents accordingly.

From the metpy documentation: [altimeter_to_station_pressure](https://github.com/Unidata/MetPy/blob/433bdd18cc807efc2507e91094776403edee5973/src/metpy/calc/basic.py#L1147-L1223)

```text

The following definitions of altimeter setting, station pressure, and sea-level pressure are taken from [Smithsonian1951]. Altimeter setting is the pressure value to which an aircraft altimeter scale is set so that it will indicate the altitude above mean sea-level of an aircraft on the ground at the location for which the value is determined. It assumes a standard atmosphere. Station pressure is the atmospheric pressure at the designated station elevation. Sea-level pressure is a pressure value obtained by the theoretical reduction of barometric pressure to sea level. It is assumed that atmosphere extends to sea level below the station and that the properties of the atmosphere are related to conditions observed at the station. This value is recorded by some surface observation stations, but not all. If the value is recorded, it can be found in the remarks section. Finding the sea-level pressure is helpful for plotting purposes and different calculations.

```

## Features

- Connects to Couchbase using credentials from a YAML file
- Queries for METAR observation documents in a specified epoch range that do not have a dataVersion value
- Computes station surface pressure from altimeter and elevation
- Updates documents with the new surface pressure value
- Updates documents to contain the altimeter pressure value
- adds a dataVersion value 0f 1.0.1

## Prerequisites

- Go 1.18+
- Couchbase Go SDK (`github.com/couchbase/gocb/v2`)
- A Couchbase instance with the required documents
- A credentials YAML file (see below)

## Credentials File

Set the environment variable `CREDENTIALS_FILE` to the path of your credentials YAML file. Example YAML structure:

```yaml
cb_host: "[localhost](couchbase://adb-cb1.gsd.esrl.noaa.gov)"
cb_user: "avid"
cb_password: "avid password"
cb_bucket: "vxdata"
cb_scope: "_default_"
cb_collection: "METAR"
```

## Usage

From the `obs_altitude_to_surface` directory, run:

```text
go run main.go --start_epoch=<an epoch> --end_epoch=<an epoch> --output_dir=<output directory>
```

Or, after building:

```text

cd <your_clone_dir/src/vxingest/utilities/obs_altitude_to_surface>
go build -o obs_altitude_to_surface
./obs_altitude_to_surface --start_epoch=<START_EPOCH> --end_epoch=<END_EPOCH> --output_dir=<output directory>
```

- `<START_EPOCH>`=Start of the epoch range (inclusive)
- `<END_EPOCH>`=End of the epoch range (inclusive) - must be greater than the <START_EPOCH>
- --output_dir=/tmp/output (or some specific directory)

Example:

```text
CREDENTIALS_FILE=/path/to/credentials.yaml go run main.go --start_epoch=1632157200 --end_epoch=1641009600 --output_dir=/tmp/output
```

## Exit Codes

- `0`: Success
- `1`: Invalid arguments or error during processing

## Logging

The program logs progress and errors to standard output.

## License

See repository root for license information.
