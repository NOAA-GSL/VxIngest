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
import math
import get_grid as gg
    
def test():
        # Grab the projection information from the test file
        grib2_file = '/Users/jeffrey.a.hamilton/VxIngest/grib2_to_cb_test/test/2125217000000'
        print("grib2 file being tested: %s" % grib2_file)

        projection = gg.getGrid(grib2_file)
        spacing, max_x, max_y = gg.getAttributes(grib2_file)

        print("projection output (pyproj object): ") 
        print(projection)

        # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
        in_proj = pyproj.Proj(proj='latlon')
        out_proj = projection

        # Example METAR station lat-lon from Portland (KPDX)
        lat, lon = 45.6, 237.4

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

        # Test to see if grid point is outside of the grid domain
        if x_stat < 0 or x_stat > max_x or y_stat < 0 or y_stat > max_y:
                print("ERROR: Station is outside the domain projection!!")


        # Grab the closest data to the station lat-lon using the x,y coordinate found
        grbs = pygrib.open(grib2_file)

        # Get timing information
        initDate = grbs[1].analDate
        validTime = grbs[1].validDate
        fcst = grbs[1].forecastTime

        print(initDate, validTime, fcst)

        ## CEILING

        #&getCeilingAGL|Geopotential Height,{'typeOfFirstFixedSurface':215},Orography
        #
        #def getCeilingAGL(dict_params):
        #    sname=dict_params['Orography']
        #    cname=dict_params['Geopotential Height']
        #    typeOfFirstFixedSurface=dict_params[{typeOfFirstFixedSurface:'215'}]
        
        #for grb in grbs:
        #        print(grb)

        surface_hgt = grbs.select(name='Orography')[0]
        print(surface_hgt)
        surface_hgt_values = surface_hgt['values']
        surface = surface_hgt_values[round(y_stat),round(x_stat)]
        ceil = grbs.select(name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
        print(ceil)
        ceil_values = ceil['values']
        ceil_msl = ceil_values[round(y_stat),round(x_stat)]
        print(surface)
        print(ceil_msl)

        # Convert to ceiling AGL and feet
        ceil_agl = (ceil_msl - surface) * 3.281
        print(ceil_agl)

        # This value matches the rounded value inside the MySQL database, extracted with the following query:
        ## select * from ceiling2.HRRR_OPS where madis_id = 2556 and time = 1626102000 and fcst_len = 3;   

        ## SURFACE PRESSURE

        sfc_pres = grbs.select(name='Surface pressure')[0]
        print(sfc_pres)
        sfc_pres_values = sfc_pres['values']
        pres = gg.interpGridBox(sfc_pres_values,x_stat,y_stat)
        #pres = sfc_pres_values[round(y_stat),round(x_stat)]

        # Convert from pascals to milibars
        pres_mb = pres / 100
        print(pres_mb)

        ## 2M TEMPERATURE

        temp = grbs.select(name='2 metre temperature')[0]
        print(temp)
        temp_values = temp['values']
        tempk = gg.interpGridBox(temp_values,x_stat,y_stat)
        #tempk = temp_values[round(y_stat),round(x_stat)]

        # Convert from Kelvin to Farenheit
        tempf = ((tempk-273.15)*9)/5 + 32
        print(tempf)

        ## 2M DEWPOINT

        dp = grbs.select(name='2 metre dewpoint temperature')[0]
        print(dp)
        dp_values = dp['values']
        #dpk = gg.interpGridBox(dp_values,x_stat,y_stat)
        dpk = dp_values[round(y_stat),round(x_stat)]

        # Convert from Kelvin to Farenheit
        dpf = ((dpk-273.15)*9)/5 + 32
        print(dpf)

        ## 2M RELATIVE HUMIDITY

        relhumid = grbs.select(name='2 metre relative humidity')[0]
        print(relhumid)
        relhumid_values = relhumid['values']
        #rh = gg.interpGridBox(relhumid_values,x_stat,y_stat)
        rh = relhumid_values[round(y_stat),round(x_stat)]

        print(rh)

        ## 2M SPECIFIC HUMIDITY

        spechumid = grbs.select(name='2 metre specific humidity')[0]
        print(spechumid)
        spechumid_values = spechumid['values']
        #rh = gg.interpGridBox(relhumid_values,x_stat,y_stat)
        sh = spechumid_values[round(y_stat),round(x_stat)]

        print(sh)

        ## WIND SPEED AND DIRECTION

        uwind = grbs.select(name='10 metre U wind component')[0]
        print(uwind)
        uwind_values = uwind['values']
        uwind_ms = gg.interpGridBox(uwind_values,x_stat,y_stat)
        #uwind_ms = uwind_values[round(y_stat),round(x_stat)]

        vwind = grbs.select(name='10 metre V wind component')[0]
        print(vwind)
        vwind_values = vwind['values']
        vwind_ms = gg.interpGridBox(vwind_values,x_stat,y_stat)
        #vwind_ms = vwind_values[round(y_stat),round(x_stat)]

        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        #wind speed then convert to mph
        ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
        ws_mph = (ws_ms/0.447) + 0.5
        print(ws_mph)

        #wind direction   - lon is the lon of the station
        theta = gg.getWindTheta(vwind,lon)
        radians = math.atan2(uwind_ms,vwind_ms)
        wd = (radians*57.2958) + theta + 180
        print(wd)   

        ## VISIBILITY

        vis = grbs.select(name='Visibility')[0]
        print(vis)
        vis_values = vis['values']
        vis_m = (vis_values[round(y_stat),round(x_stat)]) / 1609.344

        print(vis_m)

        grbs.close()             

if __name__ == "__main__":
    test()