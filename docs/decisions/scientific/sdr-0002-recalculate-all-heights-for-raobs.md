# Recalculate all heights for RAOBS

Date: 2024-10-09

## Status

Accepted

## Context

This decision was actually made a couple of years ago but we are recording it now. Because height data from radiosondes was unreliable a decision was made with the advice of senior meteorologists to always recalculate all height values. Currently all height values are calculated from the lowest reported height using the hypsometric equation and the reported temperature, pressure, and mixing ratio (derived from the specific humidity).

Once the heights are calculated, all variables, including heights, are interpolated to mandatory pressure levels using logarithmic interpolation.

## Decision

We will always recalculate heights using the [hypsometric equation](https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.thickness_hydrostatic.html), and interpolate heights and variables to mandatory levels based on the calculated heights.

## Consequences

### Positive

### Neutral

- It is important to understand this interpolation technique when comparing our RAOB data to the data in the PrepBufr files. They do not compare one to one.

### Negative
