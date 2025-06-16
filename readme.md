<pre>
  Function ConvertArray-ToDataTable {
    param($array)
    $dt = new-Object Data.datatable
    $First = $true
    foreach ($item in $array){
        $DR = $DT.NewRow()
        $Item.PsObject.get_properties() | foreach {
        If ($first) {
        $Col =  new-object Data.DataColumn
        $Col.ColumnName = $_.Name.ToString()
        $DT.Columns.Add($Col)       }
        if ($_.value -eq $null) {  
        $DR.Item($_.Name) = ""  
        }  
        ElseIf ($_.IsArray) {  
        $DR.Item($_.Name) =[string]::Join($_.value ,";")  
        }  
        Else {  
        $DR.Item($_.Name) = $_.value  
        }  
        }  
        $DT.Rows.Add($DR)  
        $First = $false  
    }
    return, $dt
  }
</pre>

