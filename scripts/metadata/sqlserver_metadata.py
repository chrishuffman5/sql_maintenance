"""
SQL Server metadata extraction module
Extracts complete schema information including tables, views, stored procedures, functions, indexes, etc.
"""

import pyodbc
from typing import Dict, List, Any
import json


class SqlServerMetadataExtractor:
    def __init__(self, server: str, database: str, auth_type: str, username: str = None, password: str = None, port: int = 1433):
        self.server = server
        self.database = database
        self.auth_type = auth_type
        self.username = username
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        """Establish connection to SQL Server"""
        if self.auth_type == 'windows':
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};DATABASE={self.database};Trusted_Connection=yes;'
        else:
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};DATABASE={self.database};UID={self.username};PWD={self.password};'

        self.connection = pyodbc.connect(conn_str)
        return self.connection

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

    def get_tables_list(self) -> List[Dict[str, str]]:
        """Get list of all tables with schema"""
        query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            t.object_id
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        tables = []
        for row in cursor.fetchall():
            tables.append({
                'schema': row.schema_name,
                'table': row.table_name,
                'object_id': row.object_id,
                'full_name': f'{row.schema_name}.{row.table_name}'
            })

        cursor.close()
        return tables

    def get_table_ddl(self, schema: str, table: str) -> str:
        """Generate CREATE TABLE DDL for a specific table"""
        ddl_parts = []
        ddl_parts.append(f"-- Table: {schema}.{table}")
        ddl_parts.append(f"CREATE TABLE [{schema}].[{table}] (")

        # Get columns
        columns = self._get_columns(schema, table)
        column_defs = []

        for col in columns:
            col_def = f"    [{col['name']}] {col['type']}"

            if col['max_length'] and col['type_name'] in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary'):
                if col['max_length'] == -1:
                    col_def += "(MAX)"
                elif col['type_name'].startswith('n'):
                    col_def += f"({col['max_length']//2})"
                else:
                    col_def += f"({col['max_length']})"

            if col['precision'] and col['type_name'] in ('decimal', 'numeric'):
                col_def += f"({col['precision']},{col['scale']})"

            if not col['is_nullable']:
                col_def += " NOT NULL"

            if col['is_identity']:
                col_def += f" IDENTITY({col['seed_value']},{col['increment_value']})"

            if col['default_definition']:
                col_def += f" DEFAULT {col['default_definition']}"

            column_defs.append(col_def)

        ddl_parts.append(",\n".join(column_defs))
        ddl_parts.append(");")

        return "\n".join(ddl_parts)

    def _get_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        # First get column info without the problematic default_definition
        query = """
        SELECT
            c.name,
            t.name AS type_name,
            CASE
                WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary', 'nvarchar', 'nchar')
                THEN CONCAT(t.name,
                    CASE
                        WHEN c.max_length = -1 THEN '(MAX)'
                        WHEN t.name LIKE 'n%' THEN CONCAT('(', c.max_length/2, ')')
                        ELSE CONCAT('(', c.max_length, ')')
                    END)
                WHEN t.name IN ('decimal', 'numeric')
                THEN CONCAT(t.name, '(', c.precision, ',', c.scale, ')')
                ELSE t.name
            END AS type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            CAST(ISNULL(ic.seed_value, 0) AS BIGINT) AS seed_value,
            CAST(ISNULL(ic.increment_value, 0) AS BIGINT) AS increment_value,
            c.column_id,
            c.default_object_id
        FROM sys.columns c
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN sys.tables tb ON c.object_id = tb.object_id
        INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
        LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE s.name = ? AND tb.name = ?
        ORDER BY c.column_id
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        columns = []
        for row in cursor.fetchall():
            # Get default definition separately to avoid ODBC type issues
            default_def = None
            if row.default_object_id:
                try:
                    def_cursor = self.connection.cursor()
                    def_cursor.execute(
                        "SELECT OBJECT_DEFINITION(?) AS def_text",
                        (row.default_object_id,)
                    )
                    def_row = def_cursor.fetchone()
                    if def_row and def_row.def_text:
                        default_def = def_row.def_text
                    def_cursor.close()
                except:
                    pass  # Skip defaults that can't be retrieved

            columns.append({
                'name': row.name,
                'type_name': row.type_name,
                'type': row.type,
                'max_length': row.max_length,
                'precision': row.precision,
                'scale': row.scale,
                'is_nullable': row.is_nullable,
                'is_identity': row.is_identity,
                'seed_value': row.seed_value,
                'increment_value': row.increment_value,
                'default_definition': default_def,
                'ordinal_position': row.column_id
            })

        cursor.close()
        return columns

    def get_primary_key(self, schema: str, table: str) -> Dict[str, Any]:
        """Get primary key information"""
        query = """
        SELECT
            kc.name AS constraint_name,
            i.type_desc AS index_type,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
        FROM sys.key_constraints kc
        INNER JOIN sys.indexes i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE kc.type = 'PK' AND s.name = ? AND t.name = ?
        GROUP BY kc.name, i.type_desc
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))
        row = cursor.fetchone()
        cursor.close()

        if row:
            return {
                'name': row.constraint_name,
                'type': row.index_type,
                'columns': row.columns.split(',')
            }
        return None

    def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all indexes for a table"""
        query = """
        SELECT
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            STRING_AGG(
                c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END,
                ','
            ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
            i.filter_definition
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND i.is_primary_key = 0 AND i.type > 0
        GROUP BY i.name, i.type_desc, i.is_unique, i.filter_definition
        ORDER BY i.name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        indexes = []
        for row in cursor.fetchall():
            indexes.append({
                'name': row.index_name,
                'type': row.index_type,
                'is_unique': row.is_unique,
                'columns': row.columns.split(','),
                'filter': row.filter_definition
            })

        cursor.close()
        return indexes

    def get_foreign_keys(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints"""
        query = """
        SELECT
            fk.name AS constraint_name,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
            OBJECT_NAME(fk.referenced_object_id) AS ref_table,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns,
            STRING_AGG(rc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
        INNER JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
        GROUP BY fk.name, fk.referenced_object_id, fk.delete_referential_action_desc, fk.update_referential_action_desc
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'name': row.constraint_name,
                'ref_schema': row.ref_schema,
                'ref_table': row.ref_table,
                'columns': row.columns.split(','),
                'ref_columns': row.ref_columns.split(','),
                'on_delete': row.delete_referential_action_desc,
                'on_update': row.update_referential_action_desc
            })

        cursor.close()
        return foreign_keys

    def get_views(self) -> List[Dict[str, str]]:
        """Get all views and their definitions"""
        query = """
        SELECT
            s.name AS schema_name,
            v.name AS view_name,
            m.definition
        FROM sys.views v
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE v.is_ms_shipped = 0
        ORDER BY s.name, v.name
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        views = []
        for row in cursor.fetchall():
            views.append({
                'schema': row.schema_name,
                'name': row.view_name,
                'definition': row.definition,
                'full_name': f'{row.schema_name}.{row.view_name}'
            })

        cursor.close()
        return views

    def get_stored_procedures(self) -> List[Dict[str, str]]:
        """Get all stored procedures"""
        query = """
        SELECT
            s.name AS schema_name,
            p.name AS procedure_name,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0
        ORDER BY s.name, p.name
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        procedures = []
        for row in cursor.fetchall():
            procedures.append({
                'schema': row.schema_name,
                'name': row.procedure_name,
                'definition': row.definition,
                'full_name': f'{row.schema_name}.{row.procedure_name}'
            })

        cursor.close()
        return procedures

    def get_functions(self) -> List[Dict[str, str]]:
        """Get all user-defined functions"""
        query = """
        SELECT
            s.name AS schema_name,
            o.name AS function_name,
            o.type_desc,
            m.definition
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
        WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0
        ORDER BY s.name, o.name
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        functions = []
        for row in cursor.fetchall():
            functions.append({
                'schema': row.schema_name,
                'name': row.function_name,
                'type': row.type_desc,
                'definition': row.definition,
                'full_name': f'{row.schema_name}.{row.function_name}'
            })

        cursor.close()
        return functions

    def extract_complete_metadata(self) -> Dict[str, Any]:
        """Extract all metadata from the database"""
        metadata = {
            'database': self.database,
            'server': self.server,
            'tables': [],
            'views': [],
            'stored_procedures': [],
            'functions': []
        }

        # Get tables with full details
        tables = self.get_tables_list()
        for table in tables:
            table_meta = {
                'schema': table['schema'],
                'name': table['table'],
                'full_name': table['full_name'],
                'ddl': self.get_table_ddl(table['schema'], table['table']),
                'columns': self._get_columns(table['schema'], table['table']),
                'primary_key': self.get_primary_key(table['schema'], table['table']),
                'indexes': self.get_indexes(table['schema'], table['table']),
                'foreign_keys': self.get_foreign_keys(table['schema'], table['table'])
            }
            metadata['tables'].append(table_meta)

        # Get views
        metadata['views'] = self.get_views()

        # Get stored procedures
        metadata['stored_procedures'] = self.get_stored_procedures()

        # Get functions
        metadata['functions'] = self.get_functions()

        return metadata
