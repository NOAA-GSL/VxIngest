SELECT RAW s.name
FROM vxdata._default.METAR s
    JOIN vxdata._default.METAR bb ON s.geo.lat BETWEEN bb.geo.bottom_right.lat AND bb.geo.top_left.lat
    AND s.geo.lon BETWEEN bb.geo.bottom_right.lon AND bb.geo.top_left.lon
WHERE bb.type="MD"
    AND bb.docType="region"
    AND bb.subset='COMMON'
    AND bb.version='V01'
    AND bb.name="E_HRRR"
    AND s.type="MD"
    AND s.docType="station"
    AND s.subset='METAR'
    AND s.version='V01'
    