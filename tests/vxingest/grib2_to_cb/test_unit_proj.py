# pylint: disable=missing-module-docstring
import os

import pyproj
import xarray as xr
from vxingest.grib2_to_cb.run_ingest_threads import VXIngest


def setup_connection():
    """test setup"""
    try:
        _vx_ingest = VXIngest()
        _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
            "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
        ).content_as[dict]["ingest_document_ids"]
        return _vx_ingest
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
        return None


def test_proj():
    """test the proj"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        ds_height_above_ground_2m = xr.open_dataset(
            "/opt/data/grib2_to_cb/hrrr_ops/input_files/2120013000018",
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "typeOfLevel": "heightAboveGround",
                    "stepType": "instant",
                    "level": 2,
                },
                "read_keys": ["projString"],
                "indexpath": "",
            },
        )
        in_proj = pyproj.Proj(proj="latlon")
        proj_string = ds_height_above_ground_2m.r2.attrs["GRIB_projString"]
        max_x = ds_height_above_ground_2m.r2.attrs["GRIB_Nx"]
        max_y = ds_height_above_ground_2m.r2.attrs["GRIB_Ny"]
        spacing = ds_height_above_ground_2m.r2.attrs["GRIB_DxInMetres"]
        latitude_of_first_grid_point_in_degrees = ds_height_above_ground_2m.r2.attrs[
            "GRIB_latitudeOfFirstGridPointInDegrees"
        ]
        longitude_of_first_grid_point_in_degrees = ds_height_above_ground_2m.r2.attrs[
            "GRIB_longitudeOfFirstGridPointInDegrees"
        ]
        proj_params = {}
        for _v in proj_string.replace(" ", "").split("+")[1:]:
            elem = _v.split("=")
            proj_params[elem[0]] = elem[1]

        in_proj = pyproj.Proj(proj="latlon")
        init_projection = pyproj.Proj(proj_params)
        latlon_proj = pyproj.Proj(proj="latlon")
        lat_0 = latitude_of_first_grid_point_in_degrees
        lon_0 = longitude_of_first_grid_point_in_degrees

        init_transformer = pyproj.Transformer.from_proj(
            proj_from=latlon_proj, proj_to=init_projection
        )
        _x, _y = init_transformer.transform(  # pylint: disable=unpacking-non-sequence
            lon_0, lat_0, radians=False
        )  # the lower left coordinates in the projection space

        # Add the proper conversion to 'fool' Proj into setting 0,0 in the lower left corner of the domain
        # NOTE: It doesn't actually do this, but it will be necessary to find x,y coordinates relative to the lower left corne
        proj_params["x_0"] = abs(_x)
        # offset the x,y points in the projection so that we get points oriented to bottm left
        proj_params["y_0"] = abs(_y)
        # Create Proj object
        out_proj = pyproj.Proj(proj_params)

        transformer = pyproj.Transformer.from_proj(proj_from=in_proj, proj_to=out_proj)
        print(
            "in_proj",
            in_proj,
            "out_proj",
            out_proj,
            "max_x",
            max_x,
            "max_y",
            max_y,
            "spacing",
            spacing,
        )
        assert (
            in_proj.definition_string()
            == "proj=longlat datum=WGS84 no_defs ellps=WGS84 towgs84=0,0,0"
        ), "in_proj definition_string is not 'proj=longlat datum=WGS84 no_defs ellps=WGS84 towgs84=0,0,0'"
        assert (
            out_proj.definition_string()
            == "proj=lcc lat_0=38.5 lon_0=262.5 lat_1=38.5 lat_2=38.5 x_0=2697520.14252193 y_0=1587306.15255666 R=6371229 units=m no_defs"
        ), "out_proj is not 'proj=lcc lat_0=38.5 lon_0=262.5 lat_1=38.5 lat_2=38.5 x_0=2697520.14252193 y_0=1587306.15255666 R=6371229 units=m no_defs'"
        assert max_x == 1799, "max_x is not 1799"
        assert max_y == 1059, "max_y is not 1059"
        assert spacing == 3000.0, "spacing is not 3000.0"

        query = f""" SELECT raw geo[0] as geo
            From `{vx_ingest.cb_credentials['bucket']}`.{vx_ingest.cb_credentials['scope']}.{vx_ingest.cb_credentials['collection']}
                    where type='MD'
                    and docType='station'
                    and subset='METAR'
                    and version='V01'
                    AND name = 'KDEN'
                    """
        result = vx_ingest.cluster.query(query)
        for row in result:
            lat = row["lat"]
            lon = row["lon"]
            if lat == -90 and lon == 180:
                continue  # don't know how to transform that station
            # pylint: disable=unpacking-non-sequence
            (
                _x,
                _y,
            ) = transformer.transform(lon, lat, radians=False)
            x_gridpoint = _x / spacing
            y_gridpoint = _y / spacing
            assert (
                x_gridpoint == 695.3172101518072
            ), "x_gridpoint is not 695.3172101518072"
            assert (
                y_gridpoint == 587.461349077341
            ), "y_gridpoint is not 587.461349077341"

    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_proj Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()
