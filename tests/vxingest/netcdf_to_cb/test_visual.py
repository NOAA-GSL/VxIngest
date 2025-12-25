import datetime as dt
import json
import os
import pathlib
import sys

import couchbase.subdocument as SD
import plotly.graph_objects as go
import pytest
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from plotly.subplots import make_subplots

from vxingest.main import connect_cb, get_credentials

"""
    _summary_
    This test will retrieve a tropoe observation from the couchbase database and then
    compare the raw observation data to the interpolated observation data that is stored in the couchbase database.
    Special note on test data:
    The test produces a plot of the raw data and the interpolated data on the local browser.
    """


@pytest.mark.integration
def test_int_tropoe_visual():
    credentials_file = os.environ["CREDENTIALS"]
    if not pathlib.Path(credentials_file).is_file():
        pytest.fail(
            "*** credentials_file file " + credentials_file + " can not be found!"
        )
    with pathlib.Path(credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
    host = _yaml_data["cb_host"]
    user = _yaml_data["cb_user"]
    password = _yaml_data["cb_password"]
    bucket = _yaml_data["cb_bucket"]
    collection = "TROPOE"
    try:
        timeout_options = ClusterTimeoutOptions(
            kv_timeout=dt.timedelta(seconds=25),
            query_timeout=dt.timedelta(seconds=120),
        )
        options = ClusterOptions(
            PasswordAuthenticator(user, password),
            timeout_options=timeout_options,
        )
        cluster = Cluster(host, options)
        collection = cluster.bucket(bucket).collection(collection)
    except Exception as _e:
        sys.exit(
            "*** builder_common.CommonVxIngest Error in connect_cb *** %s", str(_e)
        )

    # add 21600 to each document time to get 6 hours intervals
    try:
        epoch = 1622851502
        for i in range(0, 3):
            epoch = epoch + i * 21600
            doc_id = f"DD-TEST:V01:TROPOE:obs:ARM_AERI:{epoch}"
            # doc_id = "DD-TEST:V01:TROPOE:obs:1622851502"
            try:
                res = collection.lookup_in(
                    doc_id, (SD.get("data." + str(epoch) + ".raw"),)
                )
                data = res.content_as[dict](0)
                index = 0
                while index < len(data["height"]):
                    if data["height"][index] >= 5000:
                        break
                    index += 1
                raw_data = {}
                for variable in [
                    "temperature",
                    "sigma_temperature",
                    "water_vapor",
                    "sigma_water_vapor",
                    "height",
                ]:
                    raw_data[variable] = data[variable][:index]

                intrp_data = {}
                res = collection.lookup_in(
                    doc_id, (SD.get("data." + str(epoch) + ".interpolated"),)
                )
                intrp_data = res.content_as[dict](0)

                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(
                    go.Line(
                        y=raw_data["height"],
                        x=raw_data["temperature"],
                        name="raw data temperature",
                    ),
                )
                fig.add_trace(
                    go.Line(
                        y=intrp_data["levels"],
                        x=intrp_data["temperature"],
                        name="interpolated data temperature",
                    ),
                )
                fig.add_trace(
                    go.Line(
                        y=raw_data["height"],
                        x=raw_data["water_vapor"],
                        name="raw data water_vapor",
                    ),
                )
                fig.add_trace(
                    go.Line(
                        y=intrp_data["levels"],
                        x=intrp_data["water_vapor"],
                        name="interpolated data water_vapor",
                    ),
                )
                time_str = str(dt.datetime.utcfromtimestamp(epoch).isoformat())
                fig.update_layout(
                    title=f"fireweather raw data vs interpolated data {time_str}"
                )
                fig.update_traces(mode="lines+markers")
                fig.update_traces(marker=dict(size=5))
                fig.update_xaxes(title_text="temperature degC / water_vapor g/kg")
                fig.update_yaxes(title_text="height/levels meters")
                fig.show()
            except Exception as _e:
                pytest.fail(
                    f"*** builder_common.CommonVxIngest Error in connect_cb *** {str(_e)}"
                )
    finally:
        cluster.close()
        print("Connection closed")


def test_int_metar_surface_pressure_visual():
    """
    This test will retrieves a metar observation doc (hard-coded as doc_id) from the couchbase database and then
    plots the surface pressure from all stations vs station elevation, as a sanity check for surface pressure values.
    
    The plot will be displayed on the local browser.
    """
    doc_id = 'DD:V01:METAR:obs:1638266400'
    
    # establish connection
    credentials_file = os.environ["CREDENTIALS"]
    if not pathlib.Path(credentials_file).is_file():
        pytest.fail(
            "*** credentials_file file " + credentials_file + " can not be found!"
        )
    creds = get_credentials(pathlib.Path(credentials_file))
    cluster = connect_cb(creds)

    # get obs doc
    # and make a dict of surface pressure by station
    collection = cluster.bucket('vxdata').collection('METAR')
    try:
        get_doc_result = collection.get(doc_id)
    except Exception as _e:
        pytest.fail(
            "*** Failed to retrieve doc " + doc_id
        )
    obs = get_doc_result.value
    press_dict = {sta:obs['data'][sta]['Surface Pressure'] for sta in obs['data']}

    # we need elevation from metar station metadata:
    #   query the metadata docs, convert the result to list, make dict of elev by station
    # note that we are using the most recent station elevation, which may not be appropriate 
    #   depending on when the data doc is from
    metar_geo = cluster.query(
        'SELECT METAR.name, METAR.geo FROM `vxdata`.`_default`.`METAR` ' \
        'WHERE type="MD" AND version="V01" AND subset="METAR" AND docType="station"')   

    metar_geo_list = [row for row in metar_geo.rows()]
    elev_dict = {sta['name']:sta['geo'][-1]['elev'] for sta in metar_geo_list}


    # prepare to plot
    stations_to_plot = [sta for sta in press_dict if (press_dict[sta] and elev_dict[sta]<9000)]
    press_list = [press_dict[sta] for sta in stations_to_plot]
    elev_list = [elev_dict[sta] for sta in stations_to_plot]  

    fig = go.Figure(data=go.Scatter(x=elev_list, y=press_list, mode='markers', text=stations_to_plot),
                    layout=dict(title=dict(text=doc_id)))
    fig.update_xaxes(title_text="Station elevation (m)")
    fig.update_yaxes(title_text="Station surface pressure (mb)")        
    fig.show()     
                    