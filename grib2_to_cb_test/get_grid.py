"""
Program Name: get_grid.py
Contact(s): Jeff Hamilton
History Log:  Initial version
Copyright 2021 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import pygrib
import pyproj
import math


def getGrid(grib2_file):
    grbs = pygrib.open(grib2_file)
    grb = grbs[1]

    # Find the false origin easting and northing for conversion to lat-lon domain
    init_projection = pyproj.Proj(grb.projparams)
    latlon_proj = pyproj.Proj(proj='latlon')
    lat_0 = grb.latitudeOfFirstGridPointInDegrees
    lon_0=grb.longitudeOfFirstGridPointInDegrees

    transformer = pyproj.Transformer.from_proj(proj_from=latlon_proj,proj_to=init_projection)
    x, y = transformer.transform(lon_0,lat_0, radians=False)

    # Add the proper conversion to 'fool' Proj into setting 0,0 in the lower left corner of the domain
    ## NOTE: It doesn't actually do this, but it will be necessary to find x,y coordinates relative to the lower left corner
    projection_params = grb.projparams
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
    spacing = (grb.Dx)/1000

    # Grab max points in x,y directions
    max_x = grb.Nx
    max_y = grb.Ny

    grbs.close()
    return spacing, max_x, max_y

def getWindTheta(grb,lon):
    theta = 0

    proj = grb.projparams['proj']

    if proj == 'lcc':
        alattan = grb.LaDInDegrees
        elonv = grb.LoVInDegrees

        dlon = elonv-lon
        rotation = math.sin(math.radians(alattan))

        if lon > 180: lon-=360
        if lon <-180: lon+=360

        theta = -rotation*dlon

    else:
        print('Projection %s not yet supported' % proj)

    return theta

def interpGridBox(grb_values,x,y):
    xmin, xmax = math.floor(x), math.ceil(x)
    ymin, ymax = math.floor(y), math.ceil(y)

    xmin_ymin_value = grb_values[ymin,xmin]
    xmax_ymin_value = grb_values[ymin,xmax]
    xmin_ymax_value = grb_values[ymax,xmin]
    xmax_ymax_value = grb_values[ymax,xmax]

    remainder_x = x - xmin
    remainder_y = y - ymin

    interpolated_value = (remainder_x*remainder_y*xmax_ymax_value) + \
                         (remainder_x*(1-remainder_y)*xmin_ymax_value) + \
                         ((1-remainder_x)*remainder_y*xmax_ymin_value) + \
                         ((1-remainder_x)*(1-remainder_y)*xmin_ymin_value)

    return interpolated_value