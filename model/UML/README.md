# UML class diagrams of VXIngest, VxIngestManager, and builder classes

## This folder contains pdf drawings of the primary classes involved in the VXingest processing

These were generated from the code using pyreverse and graphviz (dot). Code changes so
it may become necessary to recreate these class diagrams in the future. The instructions
below list the necessary steps to install the required components and to recreate the drawings.

## Install necessary components

### PIP

 python3.9 -m pip install --upgrade pip
 pip install pydot
 pip install graphviz
 pip install -U pylint

### Graphviz brew (for mac)

 brew install Graphviz

### pyreverse

pyreverse is actually part of pylint so you get it when you install pylint. The documentation is
[pyreverse](https://pylint.pycqa.org/en/latest/user_guide/installation/index.html).

### EXTENSIONS

You need a pdf reader extension for vscode so that you can view the pdf class diagrams.
Use the vscode extension [vscode-pdf](https://marketplace.visualstudio.com/items?itemName=tomoki1207.pdf)

### running pyreverse and dot

If you run pyreverse on an entire project it is too inclusive to be illuminating. Therefore it is advisable to run it against the specific class of interest. In the case of VXingest those are the main entry classes (VXIngest), the director classes (VxIngestManager), and the builder classes (the specializations in ctc_builder, netcdf_builder, and grib2_builder).

These are the commands that were used to create the drawings in this dirwectory. It is assumed that these are run from the top level of the VXingest project.

cd ... VXingest
export PYTHONPATH=`pwd`

#### VXIngest

``` @bash
> pyreverse -ASmy -c VXIngest ctc_to_cb/run_ingest_threads
> dot -Tpdf VXIngest.dot -o model/UML/ctc_to_cb.VXIngest.pdf

> pyreverse -ASmy -c VXIngest grib2_to_cb/run_ingest_threads
> dot -Tpdf VXIngest.dot -o model/UML/grib2_to_cb.VXIngest.pdf

> pyreverse -ASmy -c VXIngest netcdf_to_cb/run_ingest_threads
> dot -Tpdf VXIngest.dot -o model/UML/netcdf_to_cb.VXIngest.pdf
```

#### VxIngestManager

``` @bash
> pyreverse -ASmy -c VxIngestManager netcdf_to_cb/vx_ingest_manager
> dot -Tpdf VxIngestManager.dot -o model/UML/netcdf_to_cb.VxIngestManager.pdf

> pyreverse -ASmy -c VxIngestManager grib2_to_cb/vx_ingest_manager
> dot -Tpdf VxIngestManager.dot -o model/UML/grib2_to_cb.VxIngestManager.pdf

> pyreverse -ASmy -c VxIngestManager ctc_to_cb/vx_ingest_manager
> dot -Tpdf VxIngestManager.dot -o model/UML/ctc_to_cb.VxIngestManager.pdf
```

#### BUILDERS

``` @bash
> pyreverse -ASmy -c NetcdfBuilder netcdf_to_cb/netcdf_builder
> dot -Tpdf NetcdfBuilder.dot -o model/UML/netcdf_to_cb.NetcdfBuilder.pdf

> pyreverse -ASmy -c NetcdfMetarObsBuilderV01 netcdf_to_cb/netcdf_builder
> dot -Tpdf NetcdfMetarObsBuilderV01.dot -o model/UML/netcdf_to_cb.NetcdfMetarObsBuilderV01.pdf

> pyreverse -ASmy -c CTCModelObsBuilderV01 ctc_to_cb/ctc_builder
> dot -Tpdf CTCModelObsBuilderV01.dot -o model/UML/ctc_to_cb.CTCModelObsBuilderV01.pdf

> pyreverse -ASmy -c GribModelBuilderV01 grib2_to_cb/grib_builder
> dot -Tpdf GribModelBuilderV01.dot -o model/UML/grib_to_cb.GribModelBuilderV01.pdf
```
