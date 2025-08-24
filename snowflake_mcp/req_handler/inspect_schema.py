import json
from typing import Any, Dict, List
from mcp.types import TextContent


class InspectSchema:
    def handle_inspect_schema(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle schema inspection."""
        table_name = arguments.get("table_name")
        schema_name = arguments.get("schema_name", "PUBLIC")
        
        if table_name:
            # Get specific table info
            query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
        else:
            # Get all tables in schema
            query = f"""
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                BYTES
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema_name}'
            ORDER BY TABLE_NAME
            """
        
        result = self.db.process_request(query)
        
        return [TextContent(
            type="text",
            text=f"Schema information:\n{json.dumps(result, indent=2, default=str)}"
        )]