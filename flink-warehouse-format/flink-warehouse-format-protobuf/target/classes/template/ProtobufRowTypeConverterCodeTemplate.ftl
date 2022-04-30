GenericRowData rd${id} = new GenericRowData(${field_size});
<#list fields as field>
    rowData.setField(${field_index}, ${field});
</#list>
${result} = rowData;