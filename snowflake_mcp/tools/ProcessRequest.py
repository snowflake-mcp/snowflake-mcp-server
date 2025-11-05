import json
import time
from typing import Any, Dict, List

class ProcessReq:
    def process_request(self, command: str) -> List[Dict[str, Any]]:
        """Execute SQL statements and return results."""
        # Split the command into individual statements
        statements = [stmt.strip() for stmt in command.split(';') if stmt.strip()]
        results = []
        conn = self.verify_link()
        start_time = time.time()

        with conn.cursor() as cursor:
            for stmt in statements:
                is_write_operation = any(
                    stmt.strip().upper().startswith(word)
                    for word in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
                )
                try:
                    if is_write_operation:
                        cursor.execute("BEGIN")
                        try:
                            cursor.execute(stmt)
                            conn.commit()
                            results.append({"statement": stmt, "affected_rows": cursor.rowcount})
                        except Exception as e:
                            conn.rollback()
                            raise
                    else:
                        cursor.execute(stmt)
                        if cursor.description:
                            columns = [col[0] for col in cursor.description]
                            rows = cursor.fetchall()
                            results.append({
                                "statement": stmt,
                                "rows": [dict(zip(columns, row)) for row in rows]
                            })
                        else:
                            results.append({"statement": stmt, "rows": []})
                except Exception as e:
                    # Note: logger not available, removed logging call
                    raise

        execution_time = time.time() - start_time
        # Note: logger not available, removed logging call
        return results