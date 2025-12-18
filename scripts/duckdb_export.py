"""
Main DuckDB export orchestrator
Coordinates metadata extraction and data export using DuckDB with S3 backend
"""

import os
import sys
import json
import duckdb
from pathlib import Path
from datetime import datetime
import uuid

# Import metadata extractors
sys.path.append(os.path.join(os.path.dirname(__file__), 'metadata'))
from sqlserver_metadata import SqlServerMetadataExtractor
from postgresql_metadata import PostgreSQLMetadataExtractor
from oracle_metadata import OracleMetadataExtractor


class DuckDBExporter:
    def __init__(self, config: dict):
        self.config = config
        self.db_type = config['database_type']
        self.server = config['server']
        self.database = config['database']
        self.port = config['port']
        self.auth_type = config['auth_type']
        self.username = config.get('username')
        self.password = config.get('password')
        self.s3_bucket_path = config['s3_bucket_path'].rstrip('/')
        self.s3_access_key = config.get('s3_access_key')
        self.s3_secret_key = config.get('s3_secret_key')
        self.s3_session_token = config.get('s3_session_token')
        self.s3_region = config.get('s3_region', 'us-east-1')
        self.aws_profile = config.get('aws_profile')

        self.metadata_extractor = None
        self.duckdb_conn = None

    def initialize_duckdb(self):
        """Initialize DuckDB connection with S3 configuration"""
        print("Initializing DuckDB with S3 support...")

        self.duckdb_conn = duckdb.connect(':memory:')

        # Install and load required extensions
        self.duckdb_conn.execute("INSTALL httpfs;")
        self.duckdb_conn.execute("LOAD httpfs;")

        # Install database-specific scanner extension
        if self.db_type == 'sqlserver':
            print("Installing nanodbc extension for SQL Server...")
            self.duckdb_conn.execute("INSTALL nanodbc FROM community;")
            self.duckdb_conn.execute("LOAD nanodbc;")
        elif self.db_type == 'postgresql':
            print("Installing PostgreSQL scanner extension...")
            self.duckdb_conn.execute("INSTALL postgres;")
            self.duckdb_conn.execute("LOAD postgres;")
        elif self.db_type == 'oracle':
            print("Installing nanodbc extension for Oracle...")
            self.duckdb_conn.execute("INSTALL nanodbc FROM community;")
            self.duckdb_conn.execute("LOAD nanodbc;")

        # Configure S3 access using DuckDB secrets
        if self.aws_profile:
            # Use AWS profile - create secret with credential_chain provider
            print(f"Using AWS profile: {self.aws_profile}")
            os.environ['AWS_PROFILE'] = self.aws_profile

            self.duckdb_conn.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    PROVIDER CREDENTIAL_CHAIN,
                    CHAIN 'config;env',
                    REGION '{self.s3_region}'
                );
            """)
            print(f"Created AWS secret using credential chain (profile: {self.aws_profile})")

        elif self.s3_access_key and self.s3_secret_key:
            # Use explicit credentials
            secret_params = f"""
                TYPE S3,
                KEY_ID '{self.s3_access_key}',
                SECRET '{self.s3_secret_key}',
                REGION '{self.s3_region}'
            """

            # Add session token if provided (for temporary credentials)
            if self.s3_session_token:
                secret_params += f",\n                SESSION_TOKEN '{self.s3_session_token}'"
                print("Creating AWS secret with temporary credentials (session token)")
            else:
                print("Creating AWS secret with static credentials")

            self.duckdb_conn.execute(f"""
                CREATE SECRET aws_secret (
                    {secret_params}
                );
            """)

        print("DuckDB initialized successfully")
        # Initialize in-memory export progress tracking table
        self._init_progress_table()

    def connect_to_source(self):
        """Connect to source database and initialize metadata extractor"""
        print(f"Connecting to {self.db_type} database: {self.database}@{self.server}...")

        if self.db_type == 'sqlserver':
            self.metadata_extractor = SqlServerMetadataExtractor(
                server=self.server,
                database=self.database,
                auth_type=self.auth_type,
                username=self.username,
                password=self.password,
                port=self.port
            )
        elif self.db_type == 'postgresql':
            self.metadata_extractor = PostgreSQLMetadataExtractor(
                server=self.server,
                database=self.database,
                username=self.username,
                password=self.password,
                port=self.port
            )
        elif self.db_type == 'oracle':
            self.metadata_extractor = OracleMetadataExtractor(
                server=self.server,
                database=self.database,
                username=self.username,
                password=self.password,
                port=self.port
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        self.metadata_extractor.connect()
        print("Connected to source database successfully")

    def extract_metadata(self):
        """Extract and save metadata"""
        print("Extracting database metadata...")

        metadata = self.metadata_extractor.extract_complete_metadata()

        # Save metadata to S3
        metadata_path = f"{self.s3_bucket_path}/metadata"
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save complete metadata JSON
        metadata_json = json.dumps(metadata, indent=2, default=str)
        metadata_file = f"{metadata_path}/metadata_{timestamp}.json"

        print(f"Saving metadata to: {metadata_file}")
        self._save_to_s3(metadata_file, metadata_json)

        # Save individual DDL files for tables
        for table in metadata['tables']:
            ddl_file = f"{metadata_path}/tables/{table['schema']}/{table['name']}.sql"
            print(f"Saving DDL: {ddl_file}")
            self._save_to_s3(ddl_file, table['ddl'])

        # Save view definitions
        for view in metadata.get('views', []):
            view_file = f"{metadata_path}/views/{view['schema']}/{view['name']}.sql"
            print(f"Saving view: {view_file}")
            self._save_to_s3(view_file, view['definition'])

        # Save procedures/functions
        for proc in metadata.get('stored_procedures', []) + metadata.get('procedures', []) + metadata.get('functions', []):
            proc_file = f"{metadata_path}/procedures/{proc['schema']}/{proc['name']}.sql"
            print(f"Saving procedure/function: {proc_file}")
            self._save_to_s3(proc_file, proc['definition'])

        print(f"Metadata extraction completed. {len(metadata['tables'])} tables processed.")
        return metadata

    def _save_to_s3(self, s3_path: str, content: str):
        """Save content to S3 using DuckDB"""
        # Escape single quotes in content
        content_escaped = content.replace("'", "''")

        # Use DuckDB to write to S3
        query = f"""
        COPY (SELECT '{content_escaped}' AS content)
        TO '{s3_path}'
        (FORMAT 'csv', HEADER false);
        """

        try:
            self.duckdb_conn.execute(query)
        except Exception as e:
            print(f"Warning: Failed to save to S3: {s3_path}. Error: {e}")
            # Fallback: save locally
            local_path = s3_path.replace('s3://', 'local_export/')
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Saved locally instead: {local_path}")

    def _init_progress_table(self):
        """Create an in-memory table to track export progress"""
        self.duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS export_logs (
                id VARCHAR,
                schema VARCHAR,
                table_name VARCHAR,
                full_name VARCHAR,
                s3_path VARCHAR,
                status VARCHAR,
                message VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            );
        """)

    def _log_export_start(self, table_info: dict, s3_path: str) -> str:
        """Insert a row marking the start of an export and return the log id"""
        log_id = str(uuid.uuid4())
        started_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        schema_escaped = table_info['schema'].replace("'", "''")
        table_escaped = table_info['name'].replace("'", "''")
        full_name_escaped = table_info['full_name'].replace("'", "''")
        s3_path_escaped = s3_path.replace("'", "''")
        self.duckdb_conn.execute(f"""
            INSERT INTO export_logs (id, schema, table_name, full_name, s3_path, status, message, started_at)
            VALUES ('{log_id}', '{schema_escaped}', '{table_escaped}', '{full_name_escaped}', '{s3_path_escaped}', 'in_progress', '', '{started_at}');
        """)
        return log_id

    def _log_export_end(self, log_id: str, status: str, message: str = ''):
        """Update the log row with final status and message"""
        finished_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        message_escaped = (message or '').replace("'", "''")
        self.duckdb_conn.execute(f"""
            UPDATE export_logs
            SET status = '{status}', message = '{message_escaped}', finished_at = '{finished_at}'
            WHERE id = '{log_id}';
        """)

    def _write_progress_to_s3(self):
        """Write the export_logs table to S3 with a timestamped filename (parquet)"""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_logs_path = f"{self.s3_bucket_path}/logs/export_log_{timestamp}.parquet"
        try:
            self.duckdb_conn.execute(f"""
                COPY (SELECT * FROM export_logs)
                TO '{s3_logs_path}'
                (FORMAT 'parquet', COMPRESSION 'ZSTD');
            """)
            print(f"Export progress log written to: {s3_logs_path}")
        except Exception as e:
            print(f"Warning: Failed to write progress log to S3: {e}")
            # Fallback: save locally as CSV
            local_path = s3_logs_path.replace('s3://', 'local_export/')
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            try:
                res = self.duckdb_conn.execute("SELECT * FROM export_logs").fetchall()
                with open(local_path.replace('.parquet', '.csv'), 'w', encoding='utf-8') as f:
                    f.write('id,schema,table_name,full_name,s3_path,status,message,started_at,finished_at\n')
                    for row in res:
                        f.write(','.join([str(col).replace(',', ' ') if col is not None else '' for col in row]) + '\n')
                print(f"Progress log saved locally instead: {local_path.replace('.parquet','.csv')}")
            except Exception as e2:
                print(f"Failed to save progress log locally: {e2}")

    def get_table_sort_order(self, table_info: dict) -> str:
        """Determine the optimal sort order for a table based on indexes"""
        schema = table_info['schema']
        table = table_info['name']

        # Check for primary key (clustered in SQL Server by default)
        if table_info.get('primary_key'):
            pk_cols = table_info['primary_key']['columns']
            print(f"Using primary key for sort order: {', '.join(pk_cols)}")
            return ', '.join([f'"{col}"' for col in pk_cols])

        # Check for clustered index
        for index in table_info.get('indexes', []):
            if index.get('type') == 'CLUSTERED':
                idx_cols = index['columns']
                print(f"Using clustered index for sort order: {', '.join(idx_cols)}")
                return ', '.join([f'"{col}"' for col in idx_cols])

        # Check for any unique index
        for index in table_info.get('indexes', []):
            if index.get('is_unique'):
                idx_cols = index['columns']
                print(f"Using unique index for sort order: {', '.join(idx_cols)}")
                return ', '.join([f'"{col}"' for col in idx_cols])

        # Default: sort by first column
        if table_info.get('columns'):
            first_col = table_info['columns'][0]['name']
            print(f"Using first column for sort order: {first_col}")
            return f'"{first_col}"'

        return ""

    def export_table_data(self, table_info: dict):
        """Export a single table's data to S3 as parquet using DuckDB scanner"""
        schema = table_info['schema']
        table = table_info['name']
        full_name = table_info['full_name']

        print(f"\nExporting table: {full_name}")

        # Determine sort order
        sort_order = self.get_table_sort_order(table_info)

        # S3 path for this table's parquet files
        table_s3_path = f"{self.s3_bucket_path}/{schema}/{table}/{table}.parquet"

        log_id = None
        try:
            print(f"Writing to: {table_s3_path}")
            # Create progress log entry for this table
            try:
                log_id = self._log_export_start(table_info, table_s3_path)
            except Exception as e:
                print(f"Warning: could not create progress log entry: {e}")

            # Use DuckDB scanner to read and export data
            if self.db_type == 'sqlserver' or self.db_type == 'oracle':
                # Build ODBC connection string for nanodbc
                if self.db_type == 'sqlserver':
                    if self.auth_type == 'windows':
                        odbc_conn = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server},{self.port};Database={self.database};Trusted_Connection=yes;"
                    else:
                        odbc_conn = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server},{self.port};Database={self.database};Uid={self.username};Pwd={self.password};"
                elif self.db_type == 'oracle':
                    odbc_conn = f"Driver={{Oracle in OraClient19Home1}};DBQ={self.server}:{self.port}/{self.database};Uid={self.username};Pwd={self.password};"

                # Use odbc_query from nanodbc extension
                # Query the specific table
                table_query = f"SELECT * FROM [{schema}].[{table}]" if self.db_type == 'sqlserver' else f'SELECT * FROM "{schema}"."{table}"'

                if sort_order:
                    table_query += f" ORDER BY {sort_order}"

                # Escape single quotes in connection string and query for DuckDB SQL literal
                odbc_conn_escaped = odbc_conn.replace("'", "''")
                table_query_escaped = table_query.replace("'", "''")

                # Use named parameters (connection=..., query=...) so the function is formatted like:
                # odbc_query(
                #   connection='Driver=...;',
                #   query='SELECT * FROM dbo.Table'
                # )
                scan_function = f"""
                    odbc_query(
                        connection='{odbc_conn_escaped}',
                        query='{table_query_escaped}'
                    )
                """

            elif self.db_type == 'postgresql':
                # PostgreSQL uses native postgres scanner (not ODBC)
                pg_conn = f"host={self.server} port={self.port} dbname={self.database} user={self.username} password={self.password}"
                pg_conn_escaped = pg_conn.replace("'", "''")
                schema_escaped = schema.replace("'", "''")
                table_escaped = table.replace("'", "''")
                scan_function = f"""
                    postgres_scan(
                        '{pg_conn_escaped}',
                        '{schema_escaped}',
                        '{table_escaped}'
                    )
                """

            else:
                raise ValueError(f"Scanner not available for {self.db_type}")

            # Build the export query
            # For SQL Server/Oracle via nanodbc, sorting is already in the query
            # For PostgreSQL, we need to add ORDER BY here
            if self.db_type == 'postgresql' and sort_order:
                export_query = f"""
                    COPY (
                        SELECT * FROM {scan_function}
                        ORDER BY {sort_order}
                    ) TO '{table_s3_path}'
                    (FORMAT 'parquet', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
                """
            else:
                export_query = f"""
                    COPY (
                        SELECT * FROM {scan_function}
                    ) TO '{table_s3_path}'
                    (FORMAT 'parquet', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
                """

            self.duckdb_conn.execute(export_query)

            print(f"✓ Table {full_name} exported successfully")
            try:
                if log_id:
                    self._log_export_end(log_id, 'success', f'Exported to {table_s3_path}')
            except Exception:
                pass

        except Exception as e:
            try:
                if log_id:
                    self._log_export_end(log_id, 'failed', str(e))
            except Exception:
                pass
            print(f"✗ Error exporting table {full_name}: {e}")
            raise

    def export_all_tables(self, metadata: dict):
        """Export all tables to S3"""
        print(f"\nStarting data export for {len(metadata['tables'])} tables...")

        total_tables = len(metadata['tables'])
        for idx, table in enumerate(metadata['tables'], 1):
            print(f"\n[{idx}/{total_tables}] Processing {table['full_name']}")
            try:
                self.export_table_data(table)
            except Exception as e:
                print(f"Failed to export {table['full_name']}: {e}")
                # Continue with next table

        try:
            self._write_progress_to_s3()
        except Exception as e:
            print(f"Warning: Failed to write progress log: {e}")

        print("\n" + "="*80)
        print("Data export completed!")
        print("="*80)

    def run(self):
        """Execute the complete export process"""
        try:
            print("="*80)
            print("DuckDB Database Export Tool")
            print("="*80)
            print(f"Source: {self.db_type} - {self.database}@{self.server}")
            print(f"Target: {self.s3_bucket_path}")
            print("="*80 + "\n")

            # Step 1: Initialize DuckDB
            self.initialize_duckdb()

            # Step 2: Connect to source database
            self.connect_to_source()

            # Step 3: Extract metadata
            metadata = self.extract_metadata()

            # Step 4: Export table data
            self.export_all_tables(metadata)

            print("\n" + "="*80)
            print("EXPORT COMPLETED SUCCESSFULLY")
            print("="*80)

        except Exception as e:
            print(f"\nERROR: Export failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)

        finally:
            # Cleanup
            if self.metadata_extractor:
                self.metadata_extractor.close()
            if self.duckdb_conn:
                self.duckdb_conn.close()


def main():
    # Get configuration from environment variable
    config_json = os.environ.get('DUCKDB_CONFIG')

    if not config_json:
        print("ERROR: DUCKDB_CONFIG environment variable not set", file=sys.stderr)
        sys.exit(1)

    try:
        config = json.loads(config_json)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in DUCKDB_CONFIG: {e}", file=sys.stderr)
        sys.exit(1)

    # Run the export
    exporter = DuckDBExporter(config)
    exporter.run()


if __name__ == '__main__':
    main()
