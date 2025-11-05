from typing import Optional, Any, Dict, List


class SearchTables:
     def search_tables(self, search_term: str, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for tables by name or comment."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            table_type,
            comment,
            row_count,
            bytes
        FROM information_schema.tables
        WHERE (UPPER(table_name) LIKE UPPER('%{search_term}%')
           OR UPPER(comment) LIKE UPPER('%{search_term}%'))
        """
        
        if database_name:
            query += f" AND table_catalog = '{database_name}'"
            
        query += " ORDER BY table_catalog, table_schema, table_name"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]