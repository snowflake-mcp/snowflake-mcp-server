from typing import Optional, Any, Dict, List


class GetTableSample:
    def get_table_sample(self, table_name: str, database_name: Optional[str] = None, 
                        schema_name: Optional[str] = None, limit: int = 10) -> Dict[str, Any]:
        """Get a sample of data from a table."""
        # Build fully qualified table name
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        parts.append(table_name)
        
        full_table_name = '.'.join(parts)
        
        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
        return {
            "table_name": full_table_name,
            "columns": columns,
            "sample_data": [dict(zip(columns, row)) for row in rows],
            "sample_size": len(rows)
        }