CREATE INDEX adv_subDocType_type_docType_version ON `default` :`vxdata`.`_default`.`METAR`(`subDocType`)
WHERE
    `type` = 'DD'
    AND `docType` = 'SUMS'
    AND `version` = 'V01'