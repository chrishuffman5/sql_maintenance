param(
    [Parameter(Mandatory)][string]$ServerInstance,
    [Parameter(Mandatory)][string]$DatabaseName,
    [Parameter(Mandatory)][int]   $TargetSizeMB,
    [string]                       $SecondaryFileGroup = 'SECONDARY',
    [int]                          $PrimaryFileCount   = 4,
    [int]                          $MaxDOP              = 8
)

# load SMO
[void][System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")

$server = New-Object Microsoft.SqlServer.Management.Smo.Server $ServerInstance
$db     = $server.Databases[$DatabaseName]
$opsDb  = $server.Databases['Operations']
if (-not $db)  { throw "Database $DatabaseName not found" }
if (-not $opsDb) { throw "'Operations' DB not found on $ServerInstance" }

#–– 1) Ensure our helper tables exist in Operations ––#
function Ensure-Table {
    param($opsDb, $name, $columnsScript)
    if (-not $opsDb.Tables[$name]) {
        $createSql = @"
CREATE TABLE dbo.$name (
    $columnsScript
);
"@
        $opsDb.ExecuteNonQuery($createSql)
        Write-Host "Created Operations.dbo.$name"
    }
}

# definitions backup
Ensure-Table -opsDb $opsDb -name 'TableDefinitions' -columnsScript @"
    SchemaName   sysname NOT NULL,
    TableName    sysname NOT NULL,
    Definition   NVARCHAR(MAX) NOT NULL,
    CapturedAt   DATETIME    NOT NULL DEFAULT GETDATE()
"@

# work queue (two phases: 1=move out, 2=move back)
Ensure-Table -opsDb $opsDb -name 'WorkQueue' -columnsScript @"
    WorkID       INT IDENTITY(1,1) PRIMARY KEY,
    SchemaName   sysname NOT NULL,
    TableName    sysname NOT NULL,
    Phase        TINYINT    NOT NULL DEFAULT 1,
    Status       NVARCHAR(20) NOT NULL DEFAULT 'Pending',
    DateAdded    DATETIME   NOT NULL DEFAULT GETDATE(),
    LastUpdated  DATETIME   NULL
"@

# work log
Ensure-Table -opsDb $opsDb -name 'WorkLog' -columnsScript @"
    LogID        INT IDENTITY(1,1) PRIMARY KEY,
    WorkID       INT       NOT NULL,
    SchemaName   sysname   NOT NULL,
    TableName    sysname   NOT NULL,
    Phase        TINYINT   NOT NULL,
    StartTime    DATETIME  NOT NULL,
    EndTime      DATETIME  NULL,
    Status       NVARCHAR(20),
    ErrorMessage NVARCHAR(4000)
"@

#–– 2) Backup all table definitions into Operations.dbo.TableDefinitions ––#
$scripterOpts = New-Object Microsoft.SqlServer.Management.Smo.ScriptingOptions
$scripterOpts.SchemaQualify       = $true
$scripterOpts.IncludeHeaders       = $true
$scripterOpts.DriAll               = $true
$scripterOpts.ScriptDrops          = $false
$scripterOpts.NoFileGroup          = $true
$scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter($server)
$scripter.Options = $scripterOpts

foreach ($tbl in $db.Tables | Where-Object { -not $_.IsSystemObject }) {
    $fullName = "$($tbl.Schema).$($tbl.Name)"
    $ddlLines  = $scripter.Script($tbl)
    $ddl       = ($ddlLines -join "`r`n").Replace("'", "''")
    $ins       = "INSERT INTO Operations.dbo.TableDefinitions (SchemaName,TableName,Definition) VALUES ('$($tbl.Schema)','$($tbl.Name)','$ddl')"
    $server.ConnectionContext.ExecuteNonQuery($ins)
    Write-Host "Backed up DDL for $fullName"
}

#–– 3) Populate queue for phase 1 (move to SECONDARY) ––#
$sizeQuery = @"
SELECT s.name AS SchemaName, t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id=s.schema_id
-- you could filter here for only large tables
"@
$allTbls = $server.ConnectionContext.ExecuteWithResults($sizeQuery).Tables[0]
foreach ($row in $allTbls.Rows) {
    $sch = $row.SchemaName; $tbl = $row.TableName
    $chk = "SELECT 1 FROM Operations.dbo.WorkQueue 
            WHERE SchemaName='$sch' AND TableName='$tbl' AND Phase=1"
    $exists = $server.ConnectionContext.ExecuteWithResults($chk).Tables[0].Rows.Count
    if (-not $exists) {
        $ins = "INSERT INTO Operations.dbo.WorkQueue (SchemaName,TableName) 
                VALUES ('$sch','$tbl')"
        $server.ConnectionContext.ExecuteNonQuery($ins)
    }
}

#–– helper to log a phase run ––#
function Invoke-TableTask {
    param($workID, $schema, $table, $phase, $actionScript)

    # mark start
    $updStart = "UPDATE Operations.dbo.WorkQueue 
                 SET Status='InProgress', LastUpdated=GETDATE() WHERE WorkID=$workID"
    $server.ConnectionContext.ExecuteNonQuery($updStart)
    $logStart = "INSERT INTO Operations.dbo.WorkLog 
                 (WorkID,SchemaName,TableName,Phase,StartTime) 
                 VALUES ($workID,'$schema','$table',$phase,GETDATE())"
    $server.ConnectionContext.ExecuteNonQuery($logStart)

    try {
        & $actionScript
        # mark complete
        $updOK = "UPDATE Operations.dbo.WorkQueue 
                  SET Status='Completed', LastUpdated=GETDATE() WHERE WorkID=$workID"
        $server.ConnectionContext.ExecuteNonQuery($updOK)

        $updLog = "UPDATE Operations.dbo.WorkLog 
                   SET EndTime=GETDATE(), Status='Success' 
                   WHERE WorkID=$workID AND Phase=$phase AND EndTime IS NULL"
        $server.ConnectionContext.ExecuteNonQuery($updLog)
    }
    catch {
        $err = $_.Exception.Message.Replace("'", "''")
        $updFail = "UPDATE Operations.dbo.WorkQueue 
                    SET Status='Failed', LastUpdated=GETDATE() 
                    WHERE WorkID=$workID"
        $failLog = "UPDATE Operations.dbo.WorkLog 
                    SET EndTime=GETDATE(), Status='Error', ErrorMessage='$err'
                    WHERE WorkID=$workID AND Phase=$phase AND EndTime IS NULL"
        $server.ConnectionContext.ExecuteNonQuery($updFail)
        $server.ConnectionContext.ExecuteNonQuery($failLog)
        throw  # re-throw if you want to halt, or comment this out to continue
    }
}

#–– 4) Phase 1 – move out to SECONDARY ––#
# ensure FG exists (same as before)...
if (-not $db.FileGroups[$SecondaryFileGroup]) {
    # ... create FG + file ...
}
# process queue rows
$q1 = $server.ConnectionContext.ExecuteWithResults(
    "SELECT WorkID, SchemaName, TableName 
     FROM Operations.dbo.WorkQueue
     WHERE Phase=1 AND Status='Pending'"
).Tables[0]
foreach ($r in $q1.Rows) {
    $wid = $r.WorkID; $sch = $r.SchemaName; $tbl = $r.TableName
    Invoke-TableTask -workID $wid -schema $sch -table $tbl -phase 1 -actionScript {
        $t = $db.Tables[$tbl, $sch]
        $ci = $t.Indexes | Where-Object { $_.IndexKeyType -eq 'DriClustered' }
        if ($ci) {
            $ci.FileGroup = $SecondaryFileGroup; $ci.Alter()
        } else {
            # heap → create temp CI same as before...
        }
    }
}

#–– 5) Shrink PRIMARY file & add new files (unchanged) ––#
# DBCC SHRINKFILE + add 3 files exactly as in the original script...

#–– 6) Phase 2 – move back to PRIMARY ––#
# bump Phase for all that succeeded in Phase 1
$server.ConnectionContext.ExecuteNonQuery(
    "UPDATE Operations.dbo.WorkQueue 
     SET Phase=2, Status='Pending', LastUpdated=NULL 
     WHERE Phase=1 AND Status='Completed'"
)
$q2 = $server.ConnectionContext.ExecuteWithResults(
    "SELECT WorkID, SchemaName, TableName 
     FROM Operations.dbo.WorkQueue
     WHERE Phase=2 AND Status='Pending'"
).Tables[0]
foreach ($r in $q2.Rows) {
    $wid = $r.WorkID; $sch = $r.SchemaName; $tbl = $r.TableName
    Invoke-TableTask -workID $wid -schema $sch -table $tbl -phase 2 -actionScript {
        $t = $db.Tables[$tbl, $sch]
        $ci = $t.Indexes | Where-Object { $_.FileGroup -eq $SecondaryFileGroup }
        if ($ci) {
            $opts = New-Object Microsoft.SqlServer.Management.Smo.RebuildIndexOptions
            $opts.MaxDegreeOfParallelism = $MaxDOP
            $ci.FileGroup = 'PRIMARY'
            $ci.Rebuild($opts)
            if ($ci.Name -like 'IX_TMP_*') { $t.Indexes[$ci.Name].Drop() }
        }
    }
}

#–– 7) Drop secondary FG (unchanged) ––#

#–– 8) FINAL: cross-check DDLs ––#
$errors = @()
foreach ($tbl in $db.Tables | Where-Object { -not $_.IsSystemObject }) {
    $backup = $server.ConnectionContext.ExecuteWithResults(
        "SELECT Definition FROM Operations.dbo.TableDefinitions
         WHERE SchemaName='$($tbl.Schema)' AND TableName='$($tbl.Name)'"
    ).Tables[0].Rows[0].Definition
    $current = ($scripter.Script($tbl) -join "`r`n").Replace("'", "''")
    if ($backup -ne $current) {
        $errors += "$($tbl.Schema).$($tbl.Name) definition mismatch"
    }
}
if ($errors.Count) {
    Write-Warning "DDL mismatches detected:`n" + ($errors -join "`n")
}
else {
    Write-Host "All table definitions match original."
}

Write-Host "=== Migration Complete! Check Operations.dbo.WorkLog for details ==="
