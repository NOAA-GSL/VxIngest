import sys

from vxingest.grib2_to_cb.grib_builder import GribModelBuilderV01
from vxingest.grib2_to_cb.raob_grib_model_builder import RaobGribModelBuilder


class RaobModelPressureLevelBuilderV01(RaobGribModelBuilder):
    """This is the builder for model data that is ingested from grib2 PRESSURE level files.
    It is a concrete builder specifically for the model raob data that are organized
    by isobaric level (pressure). While they can differ from model to model, these levels
    are mainly standardized below 100 mb to every 25 mb.
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

    def get_raw_data(self, dataset_map):
        """
        Extracts raw data from the dataset map.
        This method is overridden to handle RAOB-specific data extraction.
        Args:
            dataset_map (dict): A dictionary containing datasets keyed by their type of level.
        Returns:
            dict: A dictionary containing raw data extracted from the datasets.
        """
        raw_data = {}
        for station, data in dataset_map.items():
            # Extract relevant data for each station
            raw_data[station] = {
                "temperature": data.get("temperature"),
                "pressure": data.get("pressure"),
                "humidity": data.get("humidity"),
                "wind_speed": data.get("wind_speed"),
                "wind_direction": data.get("wind_direction"),
            }
        return raw_data

    def get_interpolated_data(self, dataset_map, raw_data):
        """
        Interpolates data from the dataset map.
        This method is overridden to handle RAOB-specific data interpolation.
        Args:
            dataset_map (dict): A dictionary containing datasets keyed by their type of level.
            raw_data (dict): A dictionary containing raw data for each station.
        Returns:
            dict: A dictionary containing interpolated data for each station.
        """
        # This method should return a dictionary keyed by station name
        # and containing the interpolated data for that station.
