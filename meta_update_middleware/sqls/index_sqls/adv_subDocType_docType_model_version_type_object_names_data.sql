CREATE INDEX adv_subDocType_docType_model_version_type_object_names_data ON `default` :`vxdata`.`_default`.`METAR`(
    `subDocType`,
    `docType`,
    `model`,
    object_names(`data`)
)
WHERE
    `type` = 'DD'
    AND `version` = 'V01'