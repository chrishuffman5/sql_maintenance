"""
Oracle metadata extraction module
Extracts complete schema information including tables, views, stored procedures, functions, indexes, etc.
Uses ODBC connection via cx_Oracle
"""

import cx_Oracle
from typing import Dict, List, Any
import json


class OracleMetadataExtractor:
    def __init__(self, server: str, database: str, username: str, password: str, port: int = 1521):
        self.server = server
        self.database = database  # Service name or SID
        self.username = username
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        """Establish connection to Oracle"""
        dsn = cx_Oracle.makedsn(self.server, self.port, service_name=self.database)
        self.connection = cx_Oracle.connect(user=self.username, password=self.password, dsn=dsn)
        return self.connection

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

    def get_tables_list(self) -> List[Dict[str, str]]:
        """Get list of all tables owned by the user"""
        query = """
        SELECT
            owner,
            table_name
        FROM all_tables
        WHERE owner = UPPER(:owner)
        ORDER BY owner, table_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': self.username})

        tables = []
        for row in cursor.fetchall():
            tables.append({
                'schema': row[0],
                'table': row[1],
                'full_name': f'{row[0]}.{row[1]}'
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

            if col['data_type'] in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') and col['data_length']:
                col_def = f'    "{col["name"]}" {col["data_type"]}({col["data_length"]})'
            elif col['data_type'] in ('NUMBER',) and col['data_precision']:
                if col['data_scale'] and col['data_scale'] > 0:
                    col_def = f'    "{col["name"]}" {col["data_type"]}({col["data_precision"]},{col["data_scale"]})'
                else:
                    col_def = f'    "{col["name"]}" {col["data_type"]}({col["data_precision"]})'

            if col['nullable'] == 'N':
                col_def += " NOT NULL"

            if col['data_default']:
                col_def += f" DEFAULT {col['data_default'].strip()}"

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
            data_length,
            data_precision,
            data_scale,
            nullable,
            data_default,
            column_id
        FROM all_tab_columns
        WHERE owner = UPPER(:owner)
            AND table_name = UPPER(:table_name)
        ORDER BY column_id
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': schema, 'table_name': table})

        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'data_length': row[2],
                'data_precision': row[3],
                'data_scale': row[4],
                'nullable': row[5],
                'data_default': row[6],
                'ordinal_position': row[7]
            })

        cursor.close()
        return columns

    def get_primary_key(self, schema: str, table: str) -> Dict[str, Any]:
        """Get primary key information"""
        query = """
        SELECT
            c.constraint_name,
            LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns
        FROM all_constraints c
        JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
            AND c.owner = cc.owner
        WHERE c.constraint_type = 'P'
            AND c.owner = UPPER(:owner)
            AND c.table_name = UPPER(:table_name)
        GROUP BY c.constraint_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': schema, 'table_name': table})
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
            i.index_name,
            i.index_type,
            i.uniqueness,
            LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
        FROM all_indexes i
        LEFT JOIN all_ind_columns ic ON i.index_name = ic.index_name
            AND i.owner = ic.index_owner
        WHERE i.owner = UPPER(:owner)
            AND i.table_name = UPPER(:table_name)
            AND NOT EXISTS (
                SELECT 1 FROM all_constraints c
                WHERE c.constraint_type = 'P'
                    AND c.index_name = i.index_name
                    AND c.owner = i.owner
            )
        GROUP BY i.index_name, i.index_type, i.uniqueness
        ORDER BY i.index_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': schema, 'table_name': table})

        indexes = []
        for row in cursor.fetchall():
            indexes.append({
                'name': row[0],
                'type': row[1],
                'is_unique': row[2] == 'UNIQUE',
                'columns': row[3].split(',') if row[3] else []
            })

        cursor.close()
        return indexes

    def get_foreign_keys(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints"""
        query = """
        SELECT
            c.constraint_name,
            r.owner AS ref_schema,
            rc.table_name AS ref_table,
            LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns,
            LISTAGG(rcc.column_name, ',') WITHIN GROUP (ORDER BY rcc.position) AS ref_columns,
            c.delete_rule
        FROM all_constraints c
        JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
            AND c.owner = cc.owner
        JOIN all_constraints r ON c.r_constraint_name = r.constraint_name
            AND c.r_owner = r.owner
        JOIN all_constraints rc ON r.constraint_name = rc.constraint_name
            AND r.owner = rc.owner
        JOIN all_cons_columns rcc ON r.constraint_name = rcc.constraint_name
            AND r.owner = rcc.owner
        WHERE c.constraint_type = 'R'
            AND c.owner = UPPER(:owner)
            AND c.table_name = UPPER(:table_name)
        GROUP BY c.constraint_name, r.owner, rc.table_name, c.delete_rule
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': schema, 'table_name': table})

        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'name': row[0],
                'ref_schema': row[1],
                'ref_table': row[2],
                'columns': row[3].split(','),
                'ref_columns': row[4].split(','),
                'on_delete': row[5]
            })

        cursor.close()
        return foreign_keys

    def get_views(self) -> List[Dict[str, str]]:
        """Get all views and their definitions"""
        query = """
        SELECT
            owner,
            view_name,
            text
        FROM all_views
        WHERE owner = UPPER(:owner)
        ORDER BY owner, view_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': self.username})

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

    def get_procedures(self) -> List[Dict[str, str]]:
        """Get all procedures"""
        query = """
        SELECT
            owner,
            object_name,
            object_type
        FROM all_procedures
        WHERE owner = UPPER(:owner)
            AND object_type IN ('PROCEDURE', 'FUNCTION')
        ORDER BY owner, object_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': self.username})

        procedures = []
        for row in cursor.fetchall():
            # Get source code
            source_query = """
            SELECT text
            FROM all_source
            WHERE owner = :owner
                AND name = :name
                AND type = :type
            ORDER BY line
            """
            src_cursor = self.connection.cursor()
            src_cursor.execute(source_query, {'owner': row[0], 'name': row[1], 'type': row[2]})
            source_lines = [src_row[0] for src_row in src_cursor.fetchall()]
            src_cursor.close()

            procedures.append({
                'schema': row[0],
                'name': row[1],
                'type': row[2],
                'definition': ''.join(source_lines),
                'full_name': f'{row[0]}.{row[1]}'
            })

        cursor.close()
        return procedures

    def get_sequences(self) -> List[Dict[str, Any]]:
        """Get all sequences"""
        query = """
        SELECT
            sequence_owner,
            sequence_name,
            min_value,
            max_value,
            increment_by,
            cycle_flag,
            cache_size,
            last_number
        FROM all_sequences
        WHERE sequence_owner = UPPER(:owner)
        ORDER BY sequence_owner, sequence_name
        """

        cursor = self.connection.cursor()
        cursor.execute(query, {'owner': self.username})

        sequences = []
        for row in cursor.fetchall():
            sequences.append({
                'schema': row[0],
                'name': row[1],
                'min_value': row[2],
                'max_value': row[3],
                'increment': row[4],
                'cycle': row[5] == 'Y',
                'cache': row[6],
                'last_number': row[7],
                'full_name': f'{row[0]}.{row[1]}'
            })

        cursor.close()
        return sequences

    def extract_complete_metadata(self) -> Dict[str, Any]:
        """Extract all metadata from the database"""
        metadata = {
            'database': self.database,
            'server': self.server,
            'schema': self.username.upper(),
            'tables': [],
            'views': [],
            'procedures': [],
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

        # Get procedures and functions
        metadata['procedures'] = self.get_procedures()

        # Get sequences
        metadata['sequences'] = self.get_sequences()

        return metadata
