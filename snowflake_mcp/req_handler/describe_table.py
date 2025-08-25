from typing import Optional, Any, Dict, List


class DescribeTables:
    def describe_table(self, table_name: str, database_name: Optional[str] = None, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        # Build fully qualified table name
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        parts.append(table_name)
        
        full_table_name = '.'.join(parts)
        
        # Get column information
        columns_query = f"""
        SELECT 
            ordinal_position,
            column_name,
            data_type,
            is_nullable,
            column_default,
            is_identity,
            comment,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        
        if database_name:
            columns_query += f" AND table_catalog = '{database_name}'"
        if schema_name:
            columns_query += f" AND table_schema = '{schema_name}'"
            
        columns_query += " ORDER BY ordinal_position"
        
        # Get table metadata
        table_query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            table_type,
            created,
            last_altered,
            comment,
            row_count,
            bytes,
            clustering_key,
            auto_clustering_on
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """
        
        if database_name:
            table_query += f" AND table_catalog = '{database_name}'"
        if schema_name:
            table_query += f" AND table_schema = '{schema_name}'"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            # Get table metadata
            cursor.execute(table_query)
            table_columns = [col[0] for col in cursor.description]
            table_rows = cursor.fetchall()
            table_info = dict(zip(table_columns, table_rows[0])) if table_rows else {}
            
            # Get column information
            cursor.execute(columns_query)
            col_columns = [col[0] for col in cursor.description]
            col_rows = cursor.fetchall()
            columns_info = [dict(zip(col_columns, row)) for row in col_rows]
            
        return {
            "table_info": table_info,
            "columns": columns_info,
            "column_count": len(columns_info)
        }