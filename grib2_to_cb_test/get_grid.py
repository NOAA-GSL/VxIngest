"""
Program Name: get_grid.py
Contact(s): Jeff Hamilton
History Log:  Initial version
Copyright 2021 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import pygrib
import pyproj


def getGrid(grib2_file):
    file = pygrib.open(grib2_file)
    grbs = [grb for grb in file]

    # Find the false origin easting and northing for conversion to lat-lon domain
    init_projection = pyproj.Proj(grbs[0].projparams)
    latlon_proj = pyproj.Proj(proj='latlon')
    lat_0 = grbs[0].latitudeOfFirstGridPointInDegrees
    lon_0=grbs[0].longitudeOfFirstGridPointInDegrees

    transformer = pyproj.Transformer.from_proj(proj_from=latlon_proj,proj_to=init_projection)
    x, y = transformer.transform(lon_0,lat_0, radians=False)

    # Add the proper conversion to 'fool' Proj into setting 0,0 in the lower left corner of the domain
    projection_params = grbs[0].projparams
    projection_params['x_0'] = abs(x)
    projection_params['y_0'] = abs(y)

    # Creat Proj object
    grid_projection = pyproj.Proj(projection_params)

    file.close()
    return grid_projection

def getSpacing(grib2_file):
    file = pygrib.open(grib2_file)
    grbs = [grb for grb in file]

    # Get grid spacing
    spacing = (grbs[0].Dx)/1000

    file.close()
    return spacing