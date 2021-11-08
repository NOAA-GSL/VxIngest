# How to import metadata
## Finding metadata in the couchbase UI...
Go to the documents page and put  "type="MD" AND docType = "matsAux" AND subset='COMMON'  AND version='V01'"
into the N1QL WHERE box. The MD:matsAux:COMMON:V01 document is the auxilliary mats metadata document.

The template for that document (and other metadata) is controlled in github. In the vxingest project the directory is
```mats_metadata_and_indexes/metadata_files/auxilliary_mats_metadata.json.```
It is better to change it in the code, check it in and then use the
mats_metadata_and_indexes/metadata_files/import_ceiling_auxilliary.sh
script to actually do the import. That script will import the ceiling_mats_metadata.json and the auxilliary_mats_metadata.json document templates as they are in the code.

Those documents don't have up to date values, because the values get updated by
the  mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh
script from the same directory. The actual values depend on the currently ingested data so what is in the git repo is really just a template.

Running the script is pretty straightforward. cd to the root of the vxingest local repo (where you cloned the VXingest repo) then
``` > ./mats_metadata_and_indexes/metadata_files/import_ceiling_auxilliary.sh credentials_file```
where credentials_file is the actual credentials file (full path). The script imports the document that you checked in.
