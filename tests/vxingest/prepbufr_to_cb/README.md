# NOTES for Prepbufr differences with legacy data

## methodology

Refer to [this test](https://github.com/NOAA-GSL/VxIngest/blob/a270e0f5851b5fa2dea7d6cce3b3323d4a4207fd/tests/vxingest/prepbufr_to_cb/test_int_read_data_from_file.py#L388).  (make link point to main after merge)

This is an integration test that compares CouchBase ingest data from a specific prepbufr file in the test data set to the same data in the legacy MYSQL data set. The Couchbase data was ingested with the VxIngest prepbufr builder and the MYSQL data was ingested with the legacy code.

The data and the test artifacts for analysis are kept in the /opt/data archive. This archive is in 
[this](https://drive.google.com/drive/folders/18YY74S8w2S0knKQRN-QxZdnfRjKxDN69?usp=drive_link) google drive folder and is named opt-data.gz. To run tests and/or view the test_artifacts you need to download this file and unpack it into /opt/data.

This test compares the data for every station in the July 31, 2024 prepbufr file that is in /opt/data/prepbufr_to_cb/input_files/242130000.gdas.t00z.prepbufr.nr.

## Results

Limits for comparison were not chosen with any specific methodology other than just estimating what appeared to be reasonable tolerances. Many stations fail to compare. After running the test...

```python
> cd .../VxIngest
> . .venv/bin/activate
> python -m pytest -s /Users/randy.pierce/VxIngest/tests/vxingest/prepbufr_to_cb/test_int_read_data_from_file.py::test_july_31_2024_0Z_data_diffs_with_legacy >/tmp/july_test.out
```

... the test results can be examined in /tmp/july_test.out
At the end of the test output are captured the stations and variables with the largest differences. These stations were analyzed by querying the databases...

MYSQL example...

```bash
export w=29839;mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z as height,t / 100 as temperature, ws / 100 as ws,wd from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${w} ORDER BY press DESC;"
```

CB example (requires that you have couchbase installed for access to cbq)...

```bash
export w=29839;cbq -q -terse --no-ssl-verify -e 'https://adb-cb1.gsd.esrl.noaa.gov:18093' -u user -p 'pwd' -s "SELECT  d.data.[\"${w}\"].pressure,d.data.[\"${w}\"].height,d.data.[\"${w}\"].temperature,d.data.[\"${w}\"].wind_speed as ws, d.data.[\"${w}\"].wind_direction as wd FROM vxdata._default.RAOB AS d WHERE d.type='DD' AND d.subset='RAOB' AND d.docType='obs' AND d.subDocType = 'prepbufr' AND d.fcstValidISO = '2024-07-31T00:00:00Z' ORDER BY d.data.[\"${w}\"].pressure DESC;" | grep -v Disabling | jq -r '.[] | "\(.pressure) \(.height) \(.temperature) \(.ws) \(.wd)"'
```

... and further analysis is done by editing the adpupa data dump for the test file from /opt/data/prepbufr_to_cb/test_artifacts/adpupa-verbose-242130000.txt. The best way to look at the adpupa data is to edit the adpupa-verbose... data dump and look for the particular subset in question for a given station by searching for 'SID *41112' and writing the section from that point to the next 'END' out to a station specific data dump i.e. /opt/data/prepbufr_to_cb/test_artifacts/41112-typ220.text. Then that smaller data file can be more easily managed. There are many of these files in the /opt/data/prepbufr_to_cb/test_artifacts directory. At least one for each station analyzed.

The results of the analysis are noted in the comment header section of the test_july_31_2024_0Z_data_diffs_with_legacy test case.

## Conclusion

1) The legacy algorithm for choosing which record to use when there are duplicate records is clearly NOT the one with the most pressure levels. There are too many cases where the MYSQL data clearly came from the one with fewer and  there is one case where the number of pressure levels was the same. I don't really understand which record to choose when there are duplicates but for now the CB code is choosing the one with the most pressure levels and if they are the same it chooses the first one. This might be able to be improved.
2) I see many cases where the interpolation of height is off. Sometimes this is because the legacy data has allowed data at a level where the q marker or program code was not allowed. Sometimes it is because there is a range of missing values that throws off the interpolation,  and sometimes the legacy data ignores a range of missing wind data whereas CB interpolates it. Interpolating wind data is highly suspect, IMO, but it should be consistent, it seems.
3) RH values are off at very cold temps. The CB algorithm is better in these cases and better matches the Adpupa data.
4) There are several cases where, for some reason, the MYSQL data is simply missing a range of data for either MAS t, z, q etc or the WIND u, v, z data. The program codes and q markers appear to me to be appropriate but I may be missing something that disqualifies the data, or the MYSQL code can just be missing data. When that happens the interpolation gets out of whack.
5) sometimes the MYSQL data will start at a level that is not a mandatory level. this can make the interpolation different.
6) there are cases where the interpolation differs (especially for wind) and the CB data is closer to the adpupa data but there is no obvious reason why the interpolation differs.
