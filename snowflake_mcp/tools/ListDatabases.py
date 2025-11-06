from typing import Any, Dict, List


class ListDatabases:

    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases accessible to the current user."""
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    