import datetime as dt
import logging

from metpy.calc import relative_humidity_from_dewpoint, wind_components
from metpy.units import units

from vxingest.partial_sums_to_cb.partial_sums_builder_parent import PartialSumsBuilder

logger = logging.getLogger(__name__)


class PartialSumsSurfaceModelObsBuilderV01(PartialSumsBuilder):
    """Build V01 surface partial-sums documents from model and observation data."""

    def __init__(self, load_spec, ingest_document):
        PartialSumsBuilder.__init__(self, load_spec, ingest_document)
        self.template = ingest_document["template"]
        self.ingest_document = None
        self.template = None
        self.subset = None
        self.model = None
        self.region = None
        self.sub_doc_type = None
        self.variable = None
        self.do_profiling = False

    def initialize_document_map(self):
        self.document_map = {}

    def get_document_map(self):
        return self.document_map

    def handle_sum(self, params_dict):
        """Calculate partial sums on matching model and observation values."""
        try:
            if "model" in params_dict:
                model_var_name = params_dict["model"]
            else:
                model_var_name = list(params_dict.keys())[0]
            if "obs" in params_dict:
                obs_var_name = params_dict["obs"]
            else:
                obs_var_name = model_var_name

            obs_vals = []
            model_vals = []
            diff_vals = []
            diff_vals_squared = []
            abs_diff_vals = []
            not_in_both = 0
            for name in self.domain_stations:
                if name in self.obs_data and name in self.model_data["data"]:
                    obs_elem = self.obs_data[name]
                    model_elem = self.model_data["data"][name]
                    if obs_var_name == "RH" or model_var_name == "RH":
                        if (
                            "RH" not in obs_elem
                            and obs_elem["DewPoint"] is not None
                            and obs_elem["Temperature"] is not None
                        ):
                            obs_elem["RH"] = (
                                relative_humidity_from_dewpoint(
                                    obs_elem["Temperature"] * units.degF,
                                    obs_elem["DewPoint"] * units.degF,
                                ).magnitude
                            ) * 100
                        if (
                            "RH" not in model_elem
                            and model_elem["DewPoint"] is not None
                            and model_elem["Temperature"] is not None
                        ):
                            model_elem["RH"] = (
                                relative_humidity_from_dewpoint(
                                    model_elem["Temperature"] * units.degF,
                                    model_elem["DewPoint"] * units.degF,
                                ).magnitude
                            ) * 100
                    if (obs_var_name == "UW" or model_var_name == "UW") or (
                        obs_var_name == "VW" or model_var_name == "VW"
                    ):
                        if (
                            ("UW" not in obs_elem or "VW" not in obs_elem)
                            and obs_elem["WS"] is not None
                            and obs_elem["WD"] is not None
                        ):
                            wind_components_t = wind_components(
                                obs_elem["WS"] * units.mph,
                                (obs_elem["WD"] - 180) * units.deg,
                            )
                            obs_elem["UW"] = wind_components_t[0].magnitude
                            obs_elem["VW"] = wind_components_t[1].magnitude
                        if (
                            ("UW" not in model_elem or "VW" not in model_elem)
                            and model_elem["WS"] is not None
                            and model_elem["WD"] is not None
                        ):
                            wind_components_t = wind_components(
                                model_elem["WS"] * units.mph,
                                (model_elem["WD"] - 180) * units.deg,
                            )
                            model_elem["UW"] = wind_components_t[0].magnitude
                            model_elem["VW"] = wind_components_t[1].magnitude
                    obs_var = obs_elem.get(obs_var_name)
                    model_var = model_elem.get(model_var_name)
                    if obs_var is not None and model_var is not None:
                        obs_vals.append(obs_var)
                        model_vals.append(model_var)
                        diff = model_var - obs_var
                        diff_vals.append(diff)
                        diff_vals_squared.append(diff * diff)
                        abs_diff_vals.append(abs(diff))
                else:
                    not_in_both += 1
            logger.debug(
                "num stations:%s num_obs:%s num_model:%s not in both count is %s",
                self.domain_stations,
                len(self.obs_data),
                len(self.model_data["data"]),
                not_in_both,
            )
            return {
                "num_recs": len(obs_vals) if obs_vals else None,
                "sum_obs": sum(obs_vals) if obs_vals else None,
                "sum_model": sum(model_vals) if model_vals else None,
                "sum_diff": sum(diff_vals) if diff_vals else None,
                "sum2_diff": sum(diff_vals_squared) if diff_vals_squared else None,
                "sum_abs": sum(abs_diff_vals) if abs_diff_vals else None,
            }
        except Exception as error:
            logger.error(
                "%s handle_sum: Exception: %s",
                self.__class__.__name__,
                str(error),
            )
            return None

    def handle_data(self, **kwargs):
        try:
            doc = kwargs["doc"]
            template_data = self.template["data"]
            data_elem = {}
            for variable in template_data:
                data_elem[variable] = self.handle_named_function(
                    template_data[variable]
                )
            doc["data"] = data_elem
            return doc
        except Exception as error:
            logger.error(
                "%s handle_data: Exception: %s",
                self.__class__.__name__,
                str(error),
            )
        return doc

    def handle_level(self, params_dict):
        return self.model_data["pressure"]

    def handle_time(self, params_dict):
        return self.model_data["fcstValidEpoch"]

    def handle_iso_time(self, params_dict):
        return dt.datetime.fromtimestamp(
            self.model_data["fcstValidEpoch"], tz=dt.UTC
        ).isoformat()

    def handle_fcst_len(self, params_dict):
        return self.model_data["fcstLen"]

    def handleWindDirU(self, params_dict):
        return self.model_data["windDirU"]

    def handleWindDirV(self, params_dict):
        return self.model_data["windDirV"]

    def handle_specific_humidity(self, params_dict):
        return self.model_data["specificHumidity"]
