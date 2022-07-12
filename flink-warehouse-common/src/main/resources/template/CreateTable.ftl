CREATE TABLE ${table.name} (
    <#list table.columns as column>
        ${column.name} ${column.type} <#if !column.nullable> NOT NULL</#if> <#if column_index + 1 != table.columns?size>,</#if>
    </#list>
    <#if table.primaryKeys??>
        , PRIMARY KEY (${table.primaryKeys}) NOT ENFORCED
    </#if>
    <#if table.watermark??>
        , WATERMARK FOR ${table.watermark.eventTimeColumn} AS ${table.watermark.strategy}
    </#if>
) WITH (
    <#if table.properties??>
        <#list table.properties?keys as key>
            '${key}' = '${table.properties[key]}' <#if key_index + 1 != table.properties?keys?size>,</#if>
        </#list>
    </#if>
)