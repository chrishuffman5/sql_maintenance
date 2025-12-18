@{
    RootModule = 'OutDuckDB.psm1'
    ModuleVersion = '0.1.0'
    GUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    Author = 'Database Export Team'
    CompanyName = 'Unknown'
    Copyright = '(c) 2025. All rights reserved.'
    Description = 'PowerShell module for exporting database schemas and data to DuckDB/S3 via Python'
    PowerShellVersion = '5.1'
    FunctionsToExport = @('Out-DuckDB')
    CmdletsToExport = @()
    VariablesToExport = '*'
    AliasesToExport = @()
    PrivateData = @{
        PSData = @{
            Tags = @('DuckDB', 'Database', 'Export', 'S3', 'SQLServer', 'PostgreSQL', 'Oracle')
            ProjectUri = ''
            ReleaseNotes = 'Initial release with Out-DuckDB function'
        }
    }
}
