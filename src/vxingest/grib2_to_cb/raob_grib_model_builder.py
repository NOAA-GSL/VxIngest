import sys

from vxingest.grib2_to_cb.grib_builder import GribModelBuilderV01


class RaobGribModelBuilder(GribModelBuilderV01):
    """
    Builder for RAOB (upper air sounding) model data ingested from GRIB2 files.
    Inherits all functionality from GribModelBuilderV01.
    Extend this class with RAOB-specific handlers or overrides as needed.
    """

    def __init__(self, load_spec, ingest_document, number_stations=None):
        super().__init__(
            load_spec,
            ingest_document,
            number_stations=number_stations
            if number_stations is not None
            else sys.maxsize,
        )
