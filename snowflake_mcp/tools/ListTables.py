from typing import Optional, Any, Dict, List

from dotenv import load_dotenv

class ListTables:
    def list_tables(self, database_name: Optional[str] = None, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all tables in a database/schema."""
        conn = self.verify_link()
        with conn.cursor() as cursor:
            if database_name and schema_name:
                cursor.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
            elif database_name:
                cursor.execute(f"SHOW TABLES IN DATABASE {database_name}")
            else:
                cursor.execute("SHOW TABLES")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]