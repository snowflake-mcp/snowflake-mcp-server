from typing import Optional, Any, Dict, List

from dotenv import load_dotenv


class SearchColumns:
    def search_columns(self, search_term: str, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for columns by name or comment."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            column_name,
            data_type,
            comment
        FROM information_schema.columns
        WHERE (UPPER(column_name) LIKE UPPER('%{search_term}%')
           OR UPPER(comment) LIKE UPPER('%{search_term}%'))
        """
        
        if database_name:
            query += f" AND table_catalog = '{database_name}'"
            
        query += " ORDER BY table_catalog, table_schema, table_name, ordinal_position"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]