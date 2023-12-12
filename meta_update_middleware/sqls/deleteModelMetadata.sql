delete FROM
    {{vxDBTARGET}}
WHERE
    type = 'MD'
    AND docType = 'matsGui'
    AND subset = 'COMMON'
    AND version = 'V01'
    AND app = '{{vxAPP}}'
    AND META().id = 'MD:matsGui:{{vxAPP}}:{{vxMODEL}}:COMMON:V01'