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
        spacing = gg.getSpacing(grib2_file)

        print("projection output (pyproj object): ") 
        print(projection)

        # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
        in_proj = pyproj.Proj(proj='latlon')
        out_proj = projection
        #out_proj = pyproj.Proj({'a': 6371229, 'b': 6371229, 'proj': 'lcc', 'lon_0': 262.5, 'lat_0': 38.5, 'lat_1': 38.5, 'lat_2': 38.5, 'x_0': 2697569.358471548, 'y_0': 1587292.3188809326})


        # Example station lat-lon (Denver International Airport in this case)
        #lat, lon = 21.138123, 237.28
        lat, lon = 40.65, 284.57

        # Find the x, y coordinate on the model grid, then round to nearest integer
        transformer = pyproj.Transformer.from_proj(proj_from=in_proj,proj_to=out_proj)
        x, y = transformer.transform(lon,lat, radians=False)
        #x, y = pyproj.transform(in_proj,projection,lon,lat)

        print(x/spacing,y/spacing)

        transformer_reverse = pyproj.Transformer.from_proj(proj_from=out_proj,proj_to=in_proj)
        lont, latt = transformer_reverse.transform(x,y, radians=False)

        print(latt,lont)


        grbs = pygrib.open(grib2_file)
        #grbs = [grb for grb in file]
        #for grb in grbs:
        #        print(n, grb.name, grb.typeOfLevel, grb.level, grb.parameterCategory, grb.parameterNumber)
        #        n = n + 1
        #grb = grbs.select(name='Geopotential Height', level=0, parameterCategory=3, parameterNumber=5, typeOfLevel='unknown')
        grb = grbs.select(name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
        print(grb)
        #for g in grb:
        #        print(g)
        #        print(g.typeOfFirstFixedSurface)
        #for key in grb.keys():
        #        #print(g.bitMapIndicator)
        #        output = 
        #        print(key)
        grbs.close() 
        
         

if __name__ == "__main__":
    test()      