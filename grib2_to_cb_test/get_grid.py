"""
Program Name: get_grid.py
Contact(s): Jeff Hamilton
History Log:  Initial version
Copyright 2021 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import pygrib
import pyproj

"""
   grbs = pygrib.open(grib2_file)
    grb = grbs[1]
    # Find the false origin easting and northing for conversion to lat-lon domain
    init_projection = pyproj.Proj(grb[1].projparams)
    latlon_proj = pyproj.Proj(proj='latlon')
    lat_0 = grb[1].latitudeOfFirstGridPointInDegrees
    lon_0=grb[1].longitudeOfFirstGridPointInDegrees
"""

def getGrid(grib2_file):
    grbs = pygrib.open(grib2_file)
    grb = grbs[1]

    # Find the false origin easting and northing for conversion to lat-lon domain
    init_projection = pyproj.Proj(grb[1].projparams)
    latlon_proj = pyproj.Proj(proj='latlon')
    lat_0 = grb[1].latitudeOfFirstGridPointInDegrees
    lon_0=grb[1].longitudeOfFirstGridPointInDegrees

    transformer = pyproj.Transformer.from_proj(proj_from=latlon_proj,proj_to=init_projection)
    x, y = transformer.transform(lon_0,lat_0, radians=False)

    # Add the proper conversion to 'fool' Proj into setting 0,0 in the lower left corner of the domain
    ## NOTE: It doesn't actually do this, but it will be necessary to find x,y coordinates relative to the lower left corner
    projection_params = grb[1].projparams
    projection_params['x_0'] = abs(x)
    projection_params['y_0'] = abs(y)

    # Creat Proj object
    grid_projection = pyproj.Proj(projection_params)

    grbs.close()
    return grid_projection

def getAttributes(grib2_file):
    grbs = pygrib.open(grib2_file)
    grb = grbs[1]

    # Get grid spacing (needed to find the proper x,y)
    spacing = (grb[1].Dx)/1000

    #Grab max points in x,y directions
    max_x = grb[1].Nx
    max_y = grb[1].Ny

    grbs.close()
    return spacing, max_x, max_y