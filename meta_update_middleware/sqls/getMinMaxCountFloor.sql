SELECT MIN(mt.fcstValidEpoch) mindate,
       MAX(mt.fcstValidEpoch) maxdate,
       COUNT(META().id) numrecs,
       FLOOR(NOW_MILLIS()/1000) updated
FROM {{vxDBTARGET}} AS mt
WHERE mt.type='DD'
    AND mt.docType='{{vxDOCTYPE}}'
    AND mt.subDocType='{{vxSUBDOCTYPE}}'
    AND mt.version='V01'
    AND mt.model='{{vxMODEL}}'