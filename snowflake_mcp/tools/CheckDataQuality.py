import json
from typing import Any, Dict, List
from mcp.types import TextContent

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CheckDataQuality:
    def handle_check_data_quality(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle data quality checks."""
        table_name = arguments["table_name"]
        schema_name = arguments.get("schema_name", "PUBLIC")
        checks = arguments.get("checks", ["null_check", "duplicate_check"])
        
        quality_results = []
        full_table_name = f"{schema_name}.{table_name}"
        
        for check in checks:
            if check == "null_check":
                # Check for nulls in each column
                query = f"""
                SELECT 
                    COLUMN_NAME,
                    SUM(CASE WHEN {full_table_name}.{'{COLUMN_NAME}'} IS NULL THEN 1 ELSE 0 END) as NULL_COUNT,
                    COUNT(*) as TOTAL_COUNT
                FROM INFORMATION_SCHEMA.COLUMNS 
                CROSS JOIN {full_table_name}
                WHERE TABLE_SCHEMA = '{schema_name}' 
                AND TABLE_NAME = '{table_name}'
                GROUP BY COLUMN_NAME
                """
                
            elif check == "duplicate_check":
                # Check for duplicate rows
                query = f"""
                SELECT COUNT(*) as TOTAL_ROWS,
                       COUNT(DISTINCT *) as UNIQUE_ROWS
                FROM {full_table_name}
                """
                
            elif check == "range_check":
                # Basic range checks for numeric columns
                query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema_name}' 
                AND TABLE_NAME = '{table_name}'
                AND DATA_TYPE IN ('NUMBER', 'FLOAT', 'INTEGER')
                """
                
            else:
                continue
            
            try:
                result = self.db.process_request(query)
                quality_results.append(f"{check.replace('_', ' ').title()}:")
                quality_results.append(json.dumps(result, indent=2, default=str))
            except Exception as e:
                quality_results.append(f"Error in {check}: {str(e)}")
        
        return [TextContent(
            type="text",
            text="\n".join(quality_results)
        )]