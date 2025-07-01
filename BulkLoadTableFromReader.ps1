[string] $SrcServer = "SERVER01"
[string] $SrcDatabase = "AdventureWorks"

[string] $DestServer = "SERVER02"
[string] $DestDatabase = "TestLoad"
[string] $DestTable = "[dbo].[OrderTracking]"
[boolean] $Truncate = $true

$CmdText = "SELECT [OrderTrackingID]
      ,[SalesOrderID]
      ,[CarrierTrackingNumber]
      ,[TrackingEventID]
      ,[EventDetails]
      ,[EventDateTime]
  FROM [AdventureWorks].[Sales].[OrderTracking]"
  
Function ConnectionString([string] $ServerName, [string] $DbName){
    "Data Source=$ServerName;Initial Catalog=$DbName;Integrated Security=True;Connect Timeout=0;”
  }
  
Function ExecuteAdoNonQuery {
Param(	[Parameter(Mandatory=$true,ValueFromPipeline=$true)]
		[String]$connectionString,
		[Parameter(Mandatory=$true,ValueFromPipeline=$true)]
		[String] $SQLStatement )
	$sqlCmd = new-object System.Data.Sqlclient.SqlCommand;
    $sqlCmd.CommandTimeout = 0;
	$sqlCmd.Connection = $connectionString;
	$sqlCmd.CommandText = $SQLStatement;
	$sqlCmd.Connection.Open();
	$sqlCmd.executeNonQuery();
	$sqlCmd.Connection.Close();
}

  If ($DestDatabase.Length –eq 0) {
    $DestDatabase = $SrcDatabase
  }
 
  If ($DestTable.Length –eq 0) {
    $DestTable = $SrcTable
  }
 
$SrcConnStr = ConnectionString $SrcServer $SrcDatabase
$SrcConn  = New-Object System.Data.SqlClient.SQLConnection($SrcConnStr)
$DestConnStr = ConnectionString $DestServer $DestDatabase
  
If ($Truncate) { 
	$TruncateSql = "TRUNCATE TABLE " + $DestTable
	ExecuteAdoNonQuery $DestConnStr $TruncateSql
}  
  
$starttime = get-date
$SqlCommand = New-Object system.Data.SqlClient.SqlCommand
$SqlCommand.Connection = $SrcConn
$SqlCommand.CommandText = $CmdText
$SqlCommand.CommandTimeout = 0
$SrcConn.Open()
[System.Data.SqlClient.SqlDataReader]$SqlReader = $SqlCommand.ExecuteReader()

Try
  {
    $bulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestConnStr, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
    $bulkCopy.DestinationTableName = $DestTable
	$bulkCopy.BatchSize = 10000
	$bulkCopy.BulkCopyTimeout = 0
    $bulkCopy.WriteToServer($sqlReader)
  }
  Catch [System.Exception]
  {
    $ex = $_.Exception
    Write-Host $ex.Message
  }
  Finally
  {
    Write-Host "Table $SrcTable in $SrcDatabase database on $SrcServer has been copied to table $DestTable in $DestDatabase database on $DestServer”
    $SqlReader.close()
    $SrcConn.Close()
    $SrcConn.Dispose()
    $bulkCopy.Close()
  }

$endtime = get-date
$endtime - $starttime
