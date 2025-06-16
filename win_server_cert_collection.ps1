<#/*
CREATE TABLE dbo.CertInventory (
    Id              INT IDENTITY PRIMARY KEY,
    ComputerName    NVARCHAR(128),
    Store           NVARCHAR(64),
    Subject         NVARCHAR(512),
    Thumbprint      NVARCHAR(64),
    NotBefore       DATETIME,
    NotAfter        DATETIME,
    SANs            NVARCHAR(MAX), -- Store as a comma-separated string or JSON
    CollectionDate  DATETIME DEFAULT GETDATE()
)
-- PowerShell script to collect SSL certificate information from Windows servers
# This script collects SSL certificate information from specified Windows servers
# and stores it in a SQL Server database for inventory and monitoring purposes.
# It retrieves certificates from the LocalMachine\My store and saves details like
*/
#>


param (
    [Parameter(Mandatory)]
    [string[]]$Servers,
    [Parameter(Mandatory)]
    [string]$SqlServer,
    [Parameter(Mandatory)]
    [string]$Database,
    [Parameter()]
    [string]$TableName = "CertInventory"
)

$certScriptBlock = {
    param($SqlServer, $Database, $TableName)

    # Create DataTable schema matching the SQL table
    $dt = New-Object System.Data.DataTable
    $dt.Columns.Add("ComputerName", [string])      | Out-Null
    $dt.Columns.Add("Store", [string])             | Out-Null
    $dt.Columns.Add("Subject", [string])           | Out-Null
    $dt.Columns.Add("Thumbprint", [string])        | Out-Null
    $dt.Columns.Add("NotBefore", [datetime])       | Out-Null
    $dt.Columns.Add("NotAfter", [datetime])        | Out-Null
    $dt.Columns.Add("SANs", [string])              | Out-Null

    $certStores = @(
        "My", "WebHosting", "Root",
        "CA", "TrustedPeople", "TrustedPublisher"
    )

    foreach ($storeName in $certStores) {
        try {
            $store = New-Object System.Security.Cryptography.X509Certificates.X509Store($storeName, "LocalMachine")
            $store.Open("ReadOnly")
            foreach ($cert in $store.Certificates) {
                $san = @()
                foreach ($ext in $cert.Extensions) {
                    if ($ext.Oid.FriendlyName -eq "Subject Alternative Name") {
                        $raw = $ext.Format($true)
                        $san = ($raw -split ",|\r?\n") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
                    }
                }
                $row = $dt.NewRow()
                $row.ComputerName = $env:COMPUTERNAME
                $row.Store        = $storeName
                $row.Subject      = $cert.Subject
                $row.Thumbprint   = $cert.Thumbprint
                $row.NotBefore    = $cert.NotBefore
                $row.NotAfter     = $cert.NotAfter
                $row.SANs         = $san -join ','
                $dt.Rows.Add($row)
            }
            $store.Close()
        } catch {
            # Handle/store errors as needed
        }
    }

    # Perform bulk insert using SqlBulkCopy
    if ($dt.Rows.Count -gt 0) {
        $connString = "Server=$SqlServer;Database=$Database;Integrated Security=SSPI;"
        $bulkCopy = New-Object Data.SqlClient.SqlBulkCopy($connString, [Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
        $bulkCopy.DestinationTableName = "dbo.$TableName"
        $bulkCopy.WriteToServer($dt)
        $bulkCopy.Close()
    }
}

# Multi-threaded remote execution
$Servers | ForEach-Object -Parallel {
    param($server, $certScriptBlock, $SqlServer, $Database, $TableName)
    Invoke-Command -ComputerName $server -ScriptBlock $certScriptBlock -ArgumentList $SqlServer, $Database, $TableName
} -ArgumentList $certScriptBlock, $SqlServer, $Database, $TableName -ThrottleLimit 10

<#
$servers = @('server1', 'server2')
$centralSqlServer = "mycentraldb.company.local"
$database = "AssetInventory"

.\Collect-ServerCerts.ps1 -Servers $servers -SqlServer $centralSqlServer -Database $database
#>