# GribModelRaobPressureBuilderV01 Design

These notes capture class design decisions for the RAOB pressure-level builder work.

## Builder Class

`GribModelRaobPressureBuilderV01` extends `GribBuilder`.
`GribModelRaobNativeBuilderV01` extends `GribBuilder`.
`GribModelRaobPressureBuilderV01` builds documents indexed on model pressure levels.
`GribModelRaobNativeBuilderV01` builds documents indexed on native model step levels.

This also requires renaming (and slightly refactoring) the original METAR-focused `GribBuilder`, since it had been the only GRIB model builder.

The class hierarchy should be normalized, with common code moved to the parent `GribBuilder`.
After this work, there will be three concrete `GribBuilder` classes.
