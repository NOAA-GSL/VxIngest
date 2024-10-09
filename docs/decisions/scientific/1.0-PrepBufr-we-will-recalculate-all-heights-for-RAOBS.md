# 1.0 PrepBufr - we will recalculate all heights for RAOBS

Date: 2024-10-01

## Status

Accepted

## Context

This decision was actually made a couple of years ago but recording it now. Because height data from radiosondes was unrelieable a decision was made with the advice of senior meteorologists to always recalculate all height values. Currently all height values are calculated from the lowest reported height using the hypsometric equation and the reported temperature, pressure, and mixing ratio (derived from the specific humidity).

Once the heights are calculated, all variables, including heights, are interpolated to mandatory pressure levels using logarithmic interpolation.

## Decision

We have chosen to always recalculate heights using hypsometric equation, and interpolate heights and variables to mandatory leves based on the calculated heights.

## Consequences

It is important to understand this interpolation technique when comparing our RAOB data to the data in the PrepBufr files. They do not compare one to one.
