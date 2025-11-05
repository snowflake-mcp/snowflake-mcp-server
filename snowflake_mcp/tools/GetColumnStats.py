from typing import Optional, Any, Dict, List


class GetColumnStats:
    def get_column_stats(self, table_name: str, column_name: str, 
                        database_name: Optional[str] = None, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get statistical information about a specific column."""
        # Build fully qualified table name
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        parts.append(table_name)
        
        full_table_name = '.'.join(parts)
        
        # Basic stats query
        stats_query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT({column_name}) as non_null_count,
            COUNT(*) - COUNT({column_name}) as null_count,
            COUNT(DISTINCT {column_name}) as distinct_count,
            MIN({column_name}) as min_value,
            MAX({column_name}) as max_value
        FROM {full_table_name}
        """
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            try:
                cursor.execute(stats_query)
                columns = [col[0] for col in cursor.description]
                row = cursor.fetchone()
                basic_stats = dict(zip(columns, row))
                
                # Try to get additional numeric stats if applicable
                numeric_stats = {}
                try:
                    numeric_query = f"""
                    SELECT 
                        AVG({column_name}) as avg_value,
                        STDDEV({column_name}) as stddev_value,
                        MEDIAN({column_name}) as median_value
                    FROM {full_table_name}
                    WHERE {column_name} IS NOT NULL
                    """
                    cursor.execute(numeric_query)
                    num_columns = [col[0] for col in cursor.description]
                    num_row = cursor.fetchone()
                    if num_row:
                        numeric_stats = dict(zip(num_columns, num_row))
                except:
                    # Column is not numeric, skip numeric stats
                    pass
                
                return {
                    "table_name": full_table_name,
                    "column_name": column_name,
                    "basic_stats": basic_stats,
                    "numeric_stats": numeric_stats
                }
                
            except Exception as e:
                # Note: logger not available, removed logging call
                raise