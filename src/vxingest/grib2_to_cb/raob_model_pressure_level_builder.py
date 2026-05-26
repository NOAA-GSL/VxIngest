import sys

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
        RaobGribModelBuilder.__init__(
            self,
            load_spec,
            ingest_document,
            number_stations=number_stations,
        )

    def get_raw_data(self, dataset_map, raw_data_template):
        """
        Extracts raw data from the dataset map.
        This method is overridden to handle RAOB-specific data extraction.
        Args:
            dataset_map (dict): A dictionary containing datasets keyed by their type of level.
        Returns:
            dict: A dictionary containing raw data extracted from the datasets.
        """
        raw_data = {}
        dataset_map_keys = list(dataset_map.keys())
        first_dataset = dataset_map.get(dataset_map_keys[0])
        steps = first_dataset[dataset_map_keys[0]].shape[0]

        for step in range(steps):
            level = first_dataset[dataset_map_keys[0]][step].item()
            raw_data[level] = {}
            for station in self.stations:
                lat = station["geo"][0]["x_gridpoint"]
                lon = station["geo"][0]["y_gridpoint"]
                wmoid = station["wmoid"]
                if lat is not None and lon is not None:
                    raw_data[wmoid] = {
                        "station_name": wmoid,
                        "latitude": lat,
                        "longitude": lon,
                    }
                    for dmkey in dataset_map_keys:
                        for var_long_name, var_name in raw_data_template[dmkey].items():
                            raw_data[level][wmoid][var_long_name] = dataset_map[dmkey][var_name][step, lat, lon].item()
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

