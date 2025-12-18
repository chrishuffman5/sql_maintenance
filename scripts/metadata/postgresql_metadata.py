"""
PostgreSQL metadata extraction module
Extracts complete schema information including tables, views, stored procedures, functions, indexes, etc.
"""

import psycopg2
from typing import Dict, List, Any
import json


class PostgreSQLMetadataExtractor:
    def __init__(self, server: str, database: str, username: str, password: str, port: int = 5432):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        """Establish connection to PostgreSQL"""
        self.connection = psycopg2.connect(
            host=self.server,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password
        )
        return self.connection

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

    def get_tables_list(self) -> List[Dict[str, str]]:
        """Get list of all tables with schema"""
        query = """
        SELECT
            schemaname AS schema_name,
            tablename AS table_name,
            schemaname || '.' || tablename AS full_name
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, tablename
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        tables = []
        for row in cursor.fetchall():
            tables.append({
                'schema': row[0],
                'table': row[1],
                'full_name': row[2]
            })

        cursor.close()
        return tables

    def get_table_ddl(self, schema: str, table: str) -> str:
        """Generate CREATE TABLE DDL for a specific table"""
        ddl_parts = []
        ddl_parts.append(f"-- Table: {schema}.{table}")
        ddl_parts.append(f'CREATE TABLE "{schema}"."{table}" (')

        # Get columns
        columns = self._get_columns(schema, table)
        column_defs = []

        for col in columns:
            col_def = f'    "{col["name"]}" {col["data_type"]}'

            if col['character_maximum_length']:
                col_def = f'    "{col["name"]}" {col["udt_name"]}({col["character_maximum_length"]})'
            elif col['numeric_precision'] and col['data_type'] in ('numeric', 'decimal'):
                col_def = f'    "{col["name"]}" {col["data_type"]}({col["numeric_precision"]},{col["numeric_scale"]})'

            if not col['is_nullable']:
                col_def += " NOT NULL"

            if col['column_default']:
                col_def += f" DEFAULT {col['column_default']}"

            column_defs.append(col_def)

        ddl_parts.append(",\n".join(column_defs))
        ddl_parts.append(");")

        return "\n".join(ddl_parts)

    def _get_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        query = """
        SELECT
            column_name,
            data_type,
            udt_name,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable::boolean,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'udt_name': row[2],
                'character_maximum_length': row[3],
                'numeric_precision': row[4],
                'numeric_scale': row[5],
                'is_nullable': row[6],
                'column_default': row[7],
                'ordinal_position': row[8]
            })

        cursor.close()
        return columns

    def get_primary_key(self, schema: str, table: str) -> Dict[str, Any]:
        """Get primary key information"""
        query = """
        SELECT
            tc.constraint_name,
            string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
        GROUP BY tc.constraint_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))
        row = cursor.fetchone()
        cursor.close()

        if row:
            return {
                'name': row[0],
                'columns': row[1].split(',')
            }
        return None

    def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all indexes for a table"""
        query = """
        SELECT
            i.relname AS index_name,
            am.amname AS index_type,
            ix.indisunique AS is_unique,
            array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ',') AS columns,
            pg_get_expr(ix.indpred, ix.indrelid) AS filter_condition
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = %s
            AND t.relname = %s
            AND NOT ix.indisprimary
        GROUP BY i.relname, am.amname, ix.indisunique, ix.indpred, ix.indrelid
        ORDER BY i.relname
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        indexes = []
        for row in cursor.fetchall():
            indexes.append({
                'name': row[0],
                'type': row[1],
                'is_unique': row[2],
                'columns': row[3].split(','),
                'filter': row[4]
            })

        cursor.close()
        return indexes

    def get_foreign_keys(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints"""
        query = """
        SELECT
            tc.constraint_name,
            ccu.table_schema AS ref_schema,
            ccu.table_name AS ref_table,
            string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS columns,
            string_agg(ccu.column_name, ',' ORDER BY kcu.ordinal_position) AS ref_columns,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
        GROUP BY tc.constraint_name, ccu.table_schema, ccu.table_name, rc.update_rule, rc.delete_rule
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'name': row[0],
                'ref_schema': row[1],
                'ref_table': row[2],
                'columns': row[3].split(','),
                'ref_columns': row[4].split(','),
                'on_update': row[5],
                'on_delete': row[6]
            })

        cursor.close()
        return foreign_keys

    def get_views(self) -> List[Dict[str, str]]:
        """Get all views and their definitions"""
        query = """
        SELECT
            schemaname AS schema_name,
            viewname AS view_name,
            definition
        FROM pg_views
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, viewname
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        views = []
        for row in cursor.fetchall():
            views.append({
                'schema': row[0],
                'name': row[1],
                'definition': row[2],
                'full_name': f'{row[0]}.{row[1]}'
            })

        cursor.close()
        return views

    def get_functions(self) -> List[Dict[str, str]]:
        """Get all user-defined functions and procedures"""
        query = """
        SELECT
            n.nspname AS schema_name,
            p.proname AS function_name,
            CASE p.prokind
                WHEN 'f' THEN 'FUNCTION'
                WHEN 'p' THEN 'PROCEDURE'
                WHEN 'a' THEN 'AGGREGATE'
                WHEN 'w' THEN 'WINDOW'
            END AS function_type,
            pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, p.proname
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        functions = []
        for row in cursor.fetchall():
            functions.append({
                'schema': row[0],
                'name': row[1],
                'type': row[2],
                'definition': row[3],
                'full_name': f'{row[0]}.{row[1]}'
            })

        cursor.close()
        return functions

    def get_sequences(self) -> List[Dict[str, Any]]:
        """Get all sequences"""
        query = """
        SELECT
            schemaname,
            sequencename,
            start_value,
            min_value,
            max_value,
            increment_by,
            cycle,
            cache_size
        FROM pg_sequences
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, sequencename
        """

        cursor = self.connection.cursor()
        cursor.execute(query)

        sequences = []
        for row in cursor.fetchall():
            sequences.append({
                'schema': row[0],
                'name': row[1],
                'start_value': row[2],
                'min_value': row[3],
                'max_value': row[4],
                'increment': row[5],
                'cycle': row[6],
                'cache': row[7],
                'full_name': f'{row[0]}.{row[1]}'
            })

        cursor.close()
        return sequences

    def extract_complete_metadata(self) -> Dict[str, Any]:
        """Extract all metadata from the database"""
        metadata = {
            'database': self.database,
            'server': self.server,
            'tables': [],
            'views': [],
            'functions': [],
            'sequences': []
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

        # Get functions and procedures
        metadata['functions'] = self.get_functions()

        # Get sequences
        metadata['sequences'] = self.get_sequences()

        return metadata
