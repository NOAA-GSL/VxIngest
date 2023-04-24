SELECT
    obs.data.AGGH,
    obs.data.AGGL,
    obs.data.AGGM,
    obs.data.AYMH,
    obs.data.AYMO,
    obs.data.AYNZ,
    obs.data.AYPY,
    obs.data.BGBW,
    obs.data.BGGH,
    obs.data.BGJN,
    obs.data.BGKK,
    obs.data.BGSF,
    obs.data.BGTL,
    obs.data.BIAR,
    obs.data.BIBD,
    obs.data.BIEG,
    obs.data.BIGR,
    obs.data.BIHN,
    obs.data.BIIS,
    obs.data.BIKF,
    obs.data.BIRK,
    obs.data.BIVM,
    obs.data.BIVO,
    obs.data.BKPR,
    obs.data.CAAW,
    obs.data.CABB,
    obs.data.CABF,
    obs.data.CABR,
    obs.data.CABT,
    obs.data.CACP,
    obs.data.CACQ,
    obs.data.CADS,
    obs.data.CAFC,
    obs.data.CAFY,
    obs.data.CAHD,
    obs.data.CAHK,
    obs.data.CAHR,
    obs.data.CAHW,
    obs.data.CAJT,
    obs.data.CAJW,
    obs.data.CAKC,
    obs.data.CAMS,
    obs.data.CAOH,
    obs.data.CAOS,
    obs.data.CAPR,
    obs.data.CAQY,
    obs.data.CARP,
    obs.data.CAVA,
    obs.data.CAWR,
    obs.data.CBBC,
    obs.data.CERM,
    obs.data.CGMG,
    obs.data.CMFM,
    obs.data.CMGB,
    obs.data.CMIN,
    obs.data.CMLU,
    obs.data.CMMY,
    obs.data.CMRF,
    obs.data.CMSI,
    obs.data.CMTH,
    obs.data.CNBB,
    obs.data.CNBI,
    obs.data.CNCD,
    obs.data.CNCO,
    obs.data.CNDT,
    obs.data.CNGC,
    obs.data.CNGH,
    obs.data.CNLB
FROM
    `vxdata`._default.METAR AS obs
WHERE
    type = "DD"
    AND docType = "obs"
    AND version = "V01"
    AND fcstValidEpoch = 1662249600