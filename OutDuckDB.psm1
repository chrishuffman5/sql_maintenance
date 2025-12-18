function Out-DuckDB {
    <#
    .SYNOPSIS
        Exports database schema and data to DuckDB/S3 format

    .DESCRIPTION
        Extracts complete database schema metadata and exports all table data to DuckDB parquet files on S3.
        Supports SQL Server, PostgreSQL, and Oracle databases.

    .PARAMETER DatabaseType
        Type of source database: SqlServer, PostgreSQL, or Oracle

    .PARAMETER ServerName
        Database server hostname or IP address

    .PARAMETER DatabaseName
        Name of the database to export

    .PARAMETER Port
        Database server port (optional, uses defaults: SQL Server=1433, PostgreSQL=5432, Oracle=1521)

    .PARAMETER AuthenticationType
        Authentication type: Windows or SQL (SQL Server), Password (PostgreSQL/Oracle)

    .PARAMETER Username
        Username for SQL authentication

    .PARAMETER Password
        Password for SQL authentication (SecureString)

    .PARAMETER S3BucketPath
        S3 bucket path (e.g., s3://mybucket/database-exports)

    .PARAMETER S3AccessKey
        AWS Access Key ID

    .PARAMETER S3SecretKey
        AWS Secret Access Key (SecureString)

    .PARAMETER S3SessionToken
        AWS Session Token (SecureString) - Required for temporary credentials from STS/IAM roles

    .PARAMETER AwsProfile
        AWS CLI profile name to use for credentials lookup. If specified, credentials will be read from ~/.aws/credentials

    .PARAMETER S3Region
        AWS region (default: us-east-1)

    .PARAMETER PythonPath
        Path to Python executable (default: python)

    .EXAMPLE
        $pwd = Read-Host -AsSecureString
        Out-DuckDB -DatabaseType SqlServer -ServerName "localhost" -DatabaseName "AdventureWorks" `
                   -AuthenticationType Windows -S3BucketPath "s3://mybucket/exports"

    .EXAMPLE
        $dbPwd = Read-Host -AsSecureString -Prompt "Database Password"
        $s3Secret = Read-Host -AsSecureString -Prompt "S3 Secret"
        Out-DuckDB -DatabaseType PostgreSQL -ServerName "pg.example.com" -DatabaseName "mydb" `
                   -AuthenticationType Password -Username "dbuser" -Password $dbPwd `
                   -S3BucketPath "s3://mybucket/pg-exports" -S3AccessKey "AKIAXXXX" -S3SecretKey $s3Secret

    .EXAMPLE
        # Using temporary credentials with session token
        $dbPwd = Read-Host -AsSecureString -Prompt "Database Password"
        $s3Secret = Read-Host -AsSecureString -Prompt "S3 Secret"
        $s3Token = Read-Host -AsSecureString -Prompt "S3 Session Token"
        Out-DuckDB -DatabaseType SqlServer -ServerName "localhost" -DatabaseName "MyDB" `
                   -AuthenticationType SQL -Username "sa" -Password $dbPwd `
                   -S3BucketPath "s3://mybucket/exports" `
                   -S3AccessKey "ASIAXXXXXXXX" -S3SecretKey $s3Secret -S3SessionToken $s3Token

    .EXAMPLE
        # Using AWS profile for credentials
        $dbPwd = Read-Host -AsSecureString -Prompt "Database Password"
        Out-DuckDB -DatabaseType SqlServer -ServerName "DESKTOP-NJQ8413" -DatabaseName "FFB" `
                   -AuthenticationType SQL -Username "myuser" -Password $dbPwd `
                   -S3BucketPath "s3://mybucket/ffb-exports" `
                   -AwsProfile "myprofile"
    #>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [ValidateSet('SqlServer', 'PostgreSQL', 'Oracle')]
        [string]$DatabaseType,

        [Parameter(Mandatory=$true)]
        [string]$ServerName,

        [Parameter(Mandatory=$true)]
        [string]$DatabaseName,

        [Parameter(Mandatory=$false)]
        [int]$Port,

        [Parameter(Mandatory=$true)]
        [ValidateSet('Windows', 'SQL', 'Password')]
        [string]$AuthenticationType,

        [Parameter(Mandatory=$false)]
        [string]$Username,

        [Parameter(Mandatory=$false)]
        [SecureString]$Password,

        [Parameter(Mandatory=$true)]
        [string]$S3BucketPath,

        [Parameter(Mandatory=$false)]
        [string]$S3AccessKey,

        [Parameter(Mandatory=$false)]
        [SecureString]$S3SecretKey,

        [Parameter(Mandatory=$false)]
        [SecureString]$S3SessionToken,

        [Parameter(Mandatory=$false)]
        [string]$AwsProfile,

        [Parameter(Mandatory=$false)]
        [string]$S3Region = 'us-east-1',

        [Parameter(Mandatory=$false)]
        [string]$PythonPath = 'python'
    )

    # Validate parameters
    if ($AuthenticationType -in @('SQL', 'Password') -and (-not $Username -or -not $Password)) {
        throw "Username and Password are required for $AuthenticationType authentication"
    }

    if ($DatabaseType -eq 'SqlServer' -and $AuthenticationType -eq 'Password') {
        throw "Use 'SQL' authentication type for SQL Server, not 'Password'"
    }

    if ($DatabaseType -in @('PostgreSQL', 'Oracle') -and $AuthenticationType -in @('Windows', 'SQL')) {
        throw "Use 'Password' authentication type for $DatabaseType"
    }

    # Validate S3 credentials: must have either profile OR explicit credentials
    if (-not $AwsProfile -and -not ($S3AccessKey -and $S3SecretKey)) {
        throw "Either -AwsProfile or both -S3AccessKey and -S3SecretKey must be provided"
    }

    if ($AwsProfile -and ($S3AccessKey -or $S3SecretKey -or $S3SessionToken)) {
        throw "Cannot specify both -AwsProfile and explicit S3 credentials (-S3AccessKey, -S3SecretKey, -S3SessionToken)"
    }

    # Set default ports if not specified
    if (-not $Port) {
        $Port = switch ($DatabaseType) {
            'SqlServer' { 1433 }
            'PostgreSQL' { 5432 }
            'Oracle' { 1521 }
        }
    }

    # Convert SecureStrings to plain text for Python (temporary)
    $dbPasswordPlain = if ($Password) {
        [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
        )
    } else { "" }

    $s3SecretPlain = if ($S3SecretKey) {
        [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($S3SecretKey)
        )
    } else { "" }

    $s3SessionTokenPlain = if ($S3SessionToken) {
        [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($S3SessionToken)
        )
    } else { "" }

    try {
        Write-Host "Starting database export: $DatabaseName from $ServerName" -ForegroundColor Cyan

        # Handle AWS profile if specified
        if ($AwsProfile) {
            Write-Host "Loading AWS credentials from profile: $AwsProfile" -ForegroundColor Yellow

            $awsCredFile = Join-Path $env:USERPROFILE ".aws\credentials"
            if (-not (Test-Path $awsCredFile)) {
                throw "AWS credentials file not found at: $awsCredFile"
            }

            # Parse AWS credentials file
            $credContent = Get-Content $awsCredFile -Raw
            $profilePattern = "\[$AwsProfile\][^\[]*aws_access_key_id\s*=\s*([^\s]+)[^\[]*aws_secret_access_key\s*=\s*([^\s]+)"
            $sessionTokenPattern = "\[$AwsProfile\][^\[]*aws_session_token\s*=\s*([^\s]+)"

            if ($credContent -match $profilePattern) {
                $S3AccessKey = $matches[1].Trim()
                $s3SecretPlain = $matches[2].Trim()

                # Check for session token
                if ($credContent -match $sessionTokenPattern) {
                    $s3SessionTokenPlain = $matches[1].Trim()
                    Write-Host "  ✓ Found temporary credentials with session token" -ForegroundColor Green
                } else {
                    Write-Host "  ✓ Found long-term credentials" -ForegroundColor Green
                }
            } else {
                throw "Profile '$AwsProfile' not found or missing required keys in $awsCredFile"
            }

            # Try to get region from config file if not specified
            if (-not $S3Region -or $S3Region -eq 'us-east-1') {
                $awsConfigFile = Join-Path $env:USERPROFILE ".aws\config"
                if (Test-Path $awsConfigFile) {
                    $configContent = Get-Content $awsConfigFile -Raw
                    $regionPattern = "\[profile $AwsProfile\][^\[]*region\s*=\s*([^\s]+)"
                    if ($configContent -match $regionPattern) {
                        $S3Region = $matches[1].Trim()
                        Write-Host "  ✓ Using region from config: $S3Region" -ForegroundColor Green
                    }
                }
            }
        }

        # Create configuration object
        $config = @{
            database_type = $DatabaseType.ToLower()
            server = $ServerName
            database = $DatabaseName
            port = $Port
            auth_type = $AuthenticationType.ToLower()
            username = $Username
            password = $dbPasswordPlain
            s3_bucket_path = $S3BucketPath
            s3_access_key = $S3AccessKey
            s3_secret_key = $s3SecretPlain
            s3_session_token = $s3SessionTokenPlain
            s3_region = $S3Region
            aws_profile = $AwsProfile
        }

        # Convert to JSON for Python
        $configJson = $config | ConvertTo-Json -Compress

        # Get the module directory
        $moduleDir = Split-Path -Parent $PSCommandPath
        $pythonScript = Join-Path $moduleDir "scripts\duckdb_export.py"

        if (-not (Test-Path $pythonScript)) {
            throw "Python script not found: $pythonScript"
        }

        # Execute Python script
        Write-Host "Executing export via Python DuckDB interface..." -ForegroundColor Yellow

        # Set environment variable without quote escaping
        $env:DUCKDB_CONFIG = $configJson
        & $PythonPath $pythonScript

        if ($LASTEXITCODE -ne 0) {
            throw "Python script failed with exit code: $LASTEXITCODE"
        }

        Write-Host "Export completed successfully!" -ForegroundColor Green

    } finally {
        # Clear sensitive data
        $dbPasswordPlain = $null
        $s3SecretPlain = $null
        $s3SessionTokenPlain = $null
        $env:DUCKDB_CONFIG = $null
        [System.GC]::Collect()
    }
}

# Export module members
Export-ModuleMember -Function Out-DuckDB
