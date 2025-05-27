import sys

from vxingest.grib2_to_cb.grib_builder import GribModelBuilderV01
from vxingest.grib2_to_cb.raob_grib_model_builder import RaobGribModelBuilder


class RaobModelNativeLevelBuilderV01(RaobGribModelBuilder):
    """This is the builder for model data that is ingested from grib2 NATIVE levels files.
    It is a concrete builder specifically for the model raob data that are organized based
    on the models preset vertical levels. This varies quite a bit from model to model
    and is dependent on the configuration set up before the model runs.
    This builder is a subclass of the GribModelBuilderV01 class.
    The primary differences between the RaobModelNativeBuilderV01 and the
    RaobModelPressureLevelBuilderV01 are the handlers that derive the pressure level.
    The pressure level needs to be interpolated according to a specific algorithm along with
    the corresponding variable values.


    Args:
        load_spec (Object): The load spec used to init the parent
        ingest_document (Object): the ingest document
        number_stations (int, optional): the maximum number of stations to process (for debugging). Defaults to sys.maxsize.

        Note: The entry point for this class is the build_document method from the ancestor class GribBuilder in grib_builder.py.
        The RaobModelPressureLevelBuilderV01 is a subclass of the RaobGribModelBuilder class.
        The RaobGribModelBuilder is a subclass of the GribModelBuilderV01 class.
        The GribModelBuilderV01 is a subclass of the GribBuilder class.
        The GribBuilder class is the base class for all grib builders and is a subclass of the
        AbstractGribBuilder class.
        The AbstractGribBuilder class is the base class for all builders.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
        GribModelBuilderV01.__init__(
            self,
            load_spec,
            ingest_document,
            number_stations=number_stations,
        )
