"""
Program Name: test_grid.py
Contact(s): Jeff Hamilton
History Log:  Initial version
Copyright 2021 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import sys
import os
import pyproj
import pygrib
import get_grid as gg
    
def test():
        # Grab the projection information from the test file
        grib2_file = '/Users/jeffrey.a.hamilton/VxIngest/grib2_to_cb_test/test/2119312000003'
        print("grib2 file being tested: %s" % grib2_file)

        projection = gg.getGrid(grib2_file)
        spacing, max_x, max_y = gg.getAttributes(grib2_file)

        print("projection output (pyproj object): ") 
        print(projection)

        # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
        in_proj = pyproj.Proj(proj='latlon')
        out_proj = projection

        # Example METAR station lat-lon from Danbury, CT (name: KDXR, id:2556)
        lat, lon = 41.37, 286.52

        # Find the x, y coordinate on the model grid, then round to nearest integer
        transformer = pyproj.Transformer.from_proj(proj_from=in_proj,proj_to=out_proj)
        x, y = transformer.transform(lon,lat, radians=False)
        x_stat, y_stat = x/spacing, y/spacing

        print(x_stat,y_stat)

        # Sanity check to make sure we can transform back to the lat-lon we started from
        transformer_reverse = pyproj.Transformer.from_proj(proj_from=out_proj,proj_to=in_proj)
        lont, latt = transformer_reverse.transform(x,y, radians=False)

        print(latt,lont)

        # Grab the lat-lon of the max grid points
        lon_max, lat_max = transformer_reverse.transform(max_x*spacing,max_y*spacing, radians=False)

        print(lat_max,lon_max)

        # Grab the closest surface height and ceiling (MSL) data to the station lat-lon using the x,y coordinate found
        grbs = pygrib.open(grib2_file)

        #&getCeilingAGL|Geopotential Height,{'typeOfFirstFixedSurface':215},Orography
        #
        #def getCeilingAGL(dict_params):
        #    sname=dict_params['Orography']
        #    cname=dict_params['Geopotential Height']
        #    typeOfFirstFixedSurface=dict_params[{typeOfFirstFixedSurface:'215'}]
        surface_hgt = grbs.select(name='Orography')[0]
        surface_hgt_values = surface_hgt['values']
        surface = surface_hgt_values[round(y_stat),round(x_stat)]
        ceil = grbs.select(name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
        ceil_values = ceil['values']
        ceil_msl = ceil_values[round(y_stat),round(x_stat)]

        grbs.close()

        print(surface,ceil_msl)

        # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
        ceil_agl = (ceil_msl - surface) * 0.32808
        print(ceil_agl)

        # This value matches the rounded value inside the MySQL database, extracted with the following query:
        ## select * from ceiling2.HRRR_OPS where madis_id = 2556 and time = 1626102000 and fcst_len = 3;        
         

if __name__ == "__main__":
    test()      