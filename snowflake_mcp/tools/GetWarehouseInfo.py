import json
from typing import Any, Dict, List


class GetWarehouseInfo:
    def get_warehouse_info(self) -> Dict[str, Any]:
        """Get comprehensive information about available warehouses including usage statistics."""
        conn = self.verify_link()
        
        with conn.cursor() as cursor:
            # Get basic warehouse info
            cursor.execute("SHOW WAREHOUSES")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            warehouses = [dict(zip(columns, row)) for row in rows]
            
            # Enhance each warehouse with usage statistics
            enhanced_warehouses = []
            total_credits = 0
            
            for warehouse in warehouses:
                warehouse_name = warehouse['name']
                enhanced = warehouse.copy()
                
                # Initialize default usage stats
                enhanced['usage_stats'] = {
                    'total_credits_used': 0,
                    'compute_credits_used': 0,
                    'cloud_services_credits_used': 0,
                    'active_days': 0,
                    'last_used': None,
                    'avg_credits_per_hour': 0
                }
                
                enhanced['load_stats'] = {
                    'avg_running_queries': 0,
                    'avg_queued_load': 0,
                    'avg_queued_provisioning': 0,
                    'avg_blocked_queries': 0
                }
                
                # Try to get usage statistics from account usage (last 30 days)
                try:
                    usage_query = f"""
                    SELECT 
                        COALESCE(SUM(credits_used), 0) as total_credits_used,
                        COALESCE(SUM(credits_used_compute), 0) as compute_credits_used,
                        COALESCE(SUM(credits_used_cloud_services), 0) as cloud_services_credits_used,
                        COUNT(DISTINCT DATE(start_time)) as active_days,
                        MAX(end_time) as last_used,
                        COALESCE(AVG(credits_used), 0) as avg_credits_per_hour
                    FROM snowflake.account_usage.warehouse_metering_history
                    WHERE warehouse_name = '{warehouse_name}' 
                    AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
                    """
                    
                    cursor.execute(usage_query)
                    usage_row = cursor.fetchone()
                    if usage_row:
                        usage_columns = [col[0] for col in cursor.description]
                        usage_data = dict(zip(usage_columns, usage_row))
                        enhanced['usage_stats'] = usage_data
                        total_credits += usage_data.get('total_credits_used', 0) or 0
                        
                except Exception as e:
                    # Note: logger not available, removed logging call
                    pass
                
                # Try to get load statistics (last 7 days)
                try:
                    load_query = f"""
                    SELECT 
                        COALESCE(AVG(avg_running), 0) as avg_running_queries,
                        COALESCE(AVG(avg_queued_load), 0) as avg_queued_load,
                        COALESCE(AVG(avg_queued_provisioning), 0) as avg_queued_provisioning,
                        COALESCE(AVG(avg_blocked), 0) as avg_blocked_queries
                    FROM snowflake.account_usage.warehouse_load_history
                    WHERE warehouse_name = '{warehouse_name}'
                    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                    """
                    
                    cursor.execute(load_query)
                    load_row = cursor.fetchone()
                    if load_row:
                        load_columns = [col[0] for col in cursor.description]
                        load_data = dict(zip(load_columns, load_row))
                        enhanced['load_stats'] = load_data
                        
                except Exception as e:
                    # Note: logger not available, removed logging call
                    pass
                
                enhanced_warehouses.append(enhanced)
            
            # Calculate summary statistics
            active_warehouses = len([w for w in enhanced_warehouses if w['state'] != 'SUSPENDED'])
            default_warehouse = next((w['name'] for w in enhanced_warehouses if w.get('is_default') == 'Y'), None)
            
            return {
                'warehouses': enhanced_warehouses,
                'summary': {
                    'total_warehouses': len(enhanced_warehouses),
                    'active_warehouses': active_warehouses,
                    'suspended_warehouses': len(enhanced_warehouses) - active_warehouses,
                    'total_credits_last_30_days': total_credits,
                    'default_warehouse': default_warehouse,
                    'analysis_period': {
                        'usage_stats': 'Last 30 days',
                        'load_stats': 'Last 7 days'
                    }
                }
            }