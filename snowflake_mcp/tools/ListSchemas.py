from typing import Optional, Any, Dict, List


class ListSchemas:
    def list_schemas(self, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all schemas in a database."""
        conn = self.verify_link()
        with conn.cursor() as cursor:
            if database_name:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {database_name}")
            else:
                cursor.execute("SHOW SCHEMAS")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]