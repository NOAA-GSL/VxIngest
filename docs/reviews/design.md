# GribModelRaobPressureBuilderV01 Design

GribModelRaobPressureBuilderV01 will extend GribBuilder.
This will necessitate also making GribModelRaobNativeBuilderV01 to build documents that are indexed on native step levels of the model.
It also necessitates renaming (and slightly refactoring) the original gribbuilder class for METARS since it was the only grib model builder
the heirarchy of the classes needs to be sorted out and common code moved to the parent GribBuilder.
There will be three GribBuilder... classes after this.

