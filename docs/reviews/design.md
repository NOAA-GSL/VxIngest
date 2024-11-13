# GribModelRaobPressureBuilderV01 Design

## Builder Class

GribModelRaobPressureBuilderV01 will extend GribBuilder.
GribModelRaobNativeBuilderV01  will extend GribBuilder.
GribModelRaobPressureBuilderV01 will build documents that are indexed on pressure levels of the model.
GribModelRaobNativeBuilderV01 will build documents that are indexed on native step levels of the model.

This also necessitates renaming (and slightly refactoring) the original GribBuilder class for METARS since it was the only grib model builder that existed.

The hierarchy of the classes needs to be sorted out and common code moved to the parent GribBuilder.
There will be three concrete GribBuilder classes after this.
