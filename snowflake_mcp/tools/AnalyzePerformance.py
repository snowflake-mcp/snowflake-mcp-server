import json
from typing import Any, Dict, List
from mcp.types import TextContent

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class AnalyzePerformance:
     def handle_analyze_performance(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle performance analysis."""
        query = arguments["query"]
        include_plan = arguments.get("explain_plan", True)
        
        analysis_results = []
        
        if include_plan:
            # Get execution plan
            explain_query = f"EXPLAIN {query}"
            plan_result = self.db.process_request(explain_query)
            analysis_results.append("Execution Plan:")
            analysis_results.append(json.dumps(plan_result, indent=2, default=str))
        
        # Get query profile (if available)
        try:
            profile_query = """
            SELECT 
                QUERY_TEXT,
                EXECUTION_TIME,
                COMPILATION_TIME,
                BYTES_SCANNED
            FROM INFORMATION_SCHEMA.QUERY_HISTORY 
            WHERE QUERY_TEXT LIKE '%{}%'
            ORDER BY START_TIME DESC 
            LIMIT 1
            """.format(query.replace("'", "''")[:100])
            
            profile_result = self.db.process_request(profile_query)
            if profile_result:
                analysis_results.append("\nRecent Performance Metrics:")
                analysis_results.append(json.dumps(profile_result, indent=2, default=str))
        except Exception as e:
            # Note: logger not available, removed logging call
            pass
        
        return [TextContent(
            type="text",
            text="\n".join(analysis_results) if analysis_results else "No performance data available"
        )]