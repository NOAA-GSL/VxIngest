delete FROM
    vxdata._default.METAR
WHERE
    type = 'MD'
    AND docType = 'matsGui'
    AND subset = 'COMMON'
    AND version = 'V01'
    AND app = 'cb-ceiling'
    AND META().id = 'MD:matsGui:cb-ceiling:{vxMODEL}:COMMON:V01'