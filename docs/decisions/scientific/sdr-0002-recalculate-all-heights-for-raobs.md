# We will recalculate all heights for radiosonde observations

Date: 2024-10-09

## Status

Accepted

## Context

This decision was actually made a couple of years ago but we are recording it now. Because height data from radiosondes (RAOBS) was unreliable a decision was made with the advice of senior meteorologists to always recalculate all height values. Currently all height values are calculated from the lowest reported height using the hypsometric equation and the reported temperature, pressure, and mixing ratio (derived from the specific humidity).

Once the heights are calculated, all variables, including heights, are interpolated to mandatory pressure levels using logarithmic interpolation.

## Decision

We will always recalculate heights using the [hypsometric equation](https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.thickness_hydrostatic.html), and interpolate heights and variables to mandatory levels based on the calculated heights.

## Consequences

### Positive

- We will have consistent height levels for computing further verification statistics
- The height levels will be derived from values we trust more than the recorded height; like temperature, pressure, and specific humidity.

### Neutral

- It is important to understand this interpolation technique when comparing our RAOB data to the data in the raw PrepBufr-formatted RAOB observation files. They do not compare one to one.

### Negative

- The height levels will be derived from other data, rather than using the measured values. This could differ from other ways the RAOB observation data is used in models.
- We will be further interpolating the observations to the mandatory levels.
