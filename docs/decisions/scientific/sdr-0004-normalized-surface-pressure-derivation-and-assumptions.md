# Normalized surface pressure derivation and assumptions

Date: 2026-02-12

### Status

Accepted

### Context

Interpolating surface pressure between model grid points does not account for (non-linear) topography between grid points, and elevation has a significant impact on surface pressure. We'd like to reduce topographic error when verifying surface pressure against obs at stations between grid points. 

In particular, in areas of complex terrain, model grid points are random relative to topography, while stations are likely to be sited in lower-lying or flatter areas, so there could be a bias of higher elevation/lower pressures in model data relative to obs.

### Decision

We will provide a few options for verifying surface pressure to better account for topography: one is "normalized surface pressure", which is derived as follows:

- Get model surface elevation at the station location using bilinear interpolation of the nearest four grid points
- Get model 2m temperature and dewpoint at the station location using bilinear interpolation of the nearest four grid points
- Compute model 2m virtual temperature from temperature, pressure, and dewpoint using MetPy [virtual_temperature_from_dewpoint](https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.virtual_temperature_from_dewpoint.html) function
- Assume a hydrostatic atmosphere and estimate model virtual temperature at the station elevation using the standard atmospheric lapse rate 
- Calculate the average temperature of the layer (between station elevation and model elevation at station location) using the two temps at the respective elevations
- Use the hypsometric equation to calculate model surface pressure at the station

References:
Hypsometric Equation Transformed to find Pressure:
P=P0\*EXP(-g(z-z0)/R\*T)
Vapor Pressure (Ambaum, 2020)
Virtual Temperature (Hobbs, 2006)

[A spreadsheet-based calculator for this derivation](https://docs.google.com/spreadsheets/d/1Ov29AHR9wE7rW-kYuOxeGx7CJhGsj3AACMYp5Ua_s3Q/edit?gid=943848829#gid=943848829)

Additional caveats/assumptions:
- We assume station elevation from station metadata is correct
- We use geopotential height of surface in model as elevation (whereas station elevation is geometric height; difference is insignificant)
- We assume station instrumentation is at 2m AGL: we use model 2m temperature and dewpoint in our calculations, but only have model surface pressure (not 2m) available to us. We assume those 2 m have an insignificant impact on the pressure value.


Note: This was implemented by adding a new GribBuilder handler in #588.

### Consequences

#### Positive

- This should provide a more realistic model surface pressure value between grid points than a straight bi-linear interpolation.
- This will be one of a few ways to verify pressure against station obs, and we can compare the results between them.

#### Neutral

- By using this approach, we are verifying model data that has had an additional level of post-processing on top of raw model output or standard post-processed model output.

#### Negative

- There are several assumptions and approximations made in this derivation.

