# VXXIngest

## Disclaimer

This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an “as is” basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.

## Purpose

The VXIngest project contains code for the purpose of ingesting meteorological data from various different sources into a document database. The data gnerated is in the form of JSON documents. These documents conform to the data model that is described in the model subdirectory.

## data model

The data model is best viewed with Hackolade. Refer to [model](model/docs/README.md) for instructions on how to access the model.

## Architecture

The architecture is briefly outlined [Here](https://docs.google.com/drawings/d/1eYlzZKAWOgKjuMVg6wVZHn0Me80TyMy5LQMUhNv-wWk/edit). 

The design follows a [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern).
There is a top level VXIngest class defined in run_ingest_threads.py that has the reponsibility of owning a thread pool of a specified number of 'Director' role VXIngestManagers and a queue of input data, as well as providing the command line interface to the factory. For a netcdf_builder the queue might be a queue of netcdf files. For a ctc builder it might be a queue of ingest templates. Each VXIngestManager has an object pool of builders. Each builder will use an ingest template and a data source to create output documents. When the queue is depleted the VXIngestManager writes the documents into a specified location where they can easily be imported to the database.

## Builders

The basic plan is to have as many builders as there are fundemental data types. Initially there are...

- netcdf - which is madis data from netcdf files. The source code for this is in netcdf_to_cb.
- grib2 - which is model output data in grib files. The source code for this is in grib2_to_cb.
- ctc - which is contigency table data derived from observations (netcdf data) and corresponding model output data. The source code for this is in ctc_to_cb.

Each builder follows a factory pattern. 

Each builder directory has a readme with more extensive information. The intent is to make the builders standalone.

### [netcdf_builder](netcdf_to_cb/README.md)

### [grib2_builder](grib2_to_cb/README.md)

### [ctc_builder](ctc_to_cb/README.md)


