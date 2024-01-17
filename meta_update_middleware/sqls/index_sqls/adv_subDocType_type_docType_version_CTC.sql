CREATE INDEX adv_subDocType_type_docType_version ON `default` :`vxdata`.`_default`.`METAR`(`subDocType`)
WHERE
    `docType` = 'CTC'
    AND `version` = 'V01'
    AND `type` = 'DD'