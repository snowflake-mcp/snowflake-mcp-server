"""
Snowflake Model Context Protocol (MCP) Server

This server enables Claude to execute SQL queries on Snowflake databases through
the Model Context Protocol (MCP). It handles connection lifecycle management,
query execution, and result formatting.
"""
import os
import asyncio
import logging
import json
import time
import csv
import pandas as pd
from typing import Optional, Any, Dict, List
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv
import mcp.server.stdio
from mcp.server import Server
from mcp.types import Tool, TextContent

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('snowflake_server')

# Load environment variables
load_dotenv()


class SnowflakeConnection:
    """Manages Snowflake database connections and query execution."""
    
    def __init__(self) -> None:
        """Initialize Snowflake connection configuration from environment variables."""
        self.config = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            # "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"), -- add this for SSO login
        }
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None
        
        # Log configuration (excluding password)
        safe_config = {k: v for k, v in self.config.items() if k != 'password'}
        logger.info(f"Initialized with config: {json.dumps(safe_config)}")
    
    def verify_link(self) -> snowflake.connector.SnowflakeConnection:
        """
        Ensure the database connection is available and valid.
        
        Returns:
            A valid Snowflake connection
            
        Raises:
            Exception: If connection cannot be established
        """
        try:
            # Create new connection if needed
            if self.conn is None:
                logger.info("Creating new Snowflake connection...")
                self.conn = snowflake.connector.connect(
                    **self.config,
                    client_session_keep_alive=True,
                    network_timeout=15,
                    login_timeout=15
                )
                self.conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                logger.info("New connection established and configured")
            
            # Test if connection is valid
            try:
                self.conn.cursor().execute("SELECT 1")
            except:
                logger.info("Connection lost, reconnecting...")
                self.conn = None
                return self.verify_link()
                
            return self.conn
            
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            raise
    
    def process_request(self, command: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return the results.
        
        Args:
            command: SQL query to execute
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            Exception: If query execution fails
        """
        start_time = time.time()
        command_preview = command[:200] + "..." if len(command) > 200 else command
        logger.info(f"Executing query: {command_preview}")
        
        try:
            conn = self.verify_link()
            
            # Determine if this is a data modification query
            is_write_operation = any(
                command.strip().upper().startswith(word) 
                for word in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
            )
            
            with conn.cursor() as cursor:
                if is_write_operation:
                    # Use transaction for write operations
                    cursor.execute("BEGIN")
                    try:
                        cursor.execute(command)
                        conn.commit()
                        execution_time = time.time() - start_time
                        logger.info(f"Write query executed in {execution_time:.2f}s")
                        return [{"affected_rows": cursor.rowcount}]
                    except Exception as e:
                        conn.rollback()
                        raise
                else:
                    # Read operations
                    cursor.execute(command)
                    execution_time = time.time() - start_time
                    
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        results = [dict(zip(columns, row)) for row in rows]
                        logger.info(f"Read query returned {len(results)} rows in {execution_time:.2f}s")
                        return results
                    
                    logger.info(f"Read query executed in {execution_time:.2f}s (no results)")
                    return []
                    
        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"SQL Error: {str(e)}")
            logger.error(f"Error Code: {getattr(e, 'errno', 'unknown')}")
            raise
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise
    
    def cleanup(self) -> None:
        """Safely close the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.conn = None


class SnowflakeServer(Server):
    """MCP server that handles Snowflake database operations."""
    
    def __init__(self) -> None:
        """Initialize the MCP server with a Snowflake connection."""
        super().__init__(name="snowflake-server")
        self.db = SnowflakeConnection()
        logger.info("SnowflakeServer initialized")

        @self.list_tools()
        async def get_supported_operations():
            """Return list of available tools."""
            return [
                Tool(
                    name="execute_query",
                    description="Execute a SQL query on Snowflake",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="inspect_schema",
                    description="Get database schema information",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Specific table name to inspect (optional)"
                            },
                            "schema_name": {
                                "type": "string", 
                                "description": "Schema name to inspect (optional)"
                            }
                        }
                    }
                ),
                Tool(
                    name="analyze_performance",
                    description="Analyze query performance and suggest optimizations",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to analyze"
                            },
                            "explain_plan": {
                                "type": "boolean",
                                "description": "Include execution plan",
                                "default": True
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="check_data_quality",
                    description="Run data quality checks on tables",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Table name to check"
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)"
                            },
                            "checks": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "enum": ["null_check", "duplicate_check", "range_check", "format_check"]
                                },
                                "description": "Types of checks to perform",
                                "default": ["null_check", "duplicate_check"]
                            }
                        },
                        "required": ["table_name"]
                    }
                ),
                Tool(
                    name="execute_batch",
                    description="Execute multiple queries in sequence",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "queries": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "query": {"type": "string"},
                                        "depends_on": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["name", "query"]
                                },
                                "description": "List of named queries with dependencies"
                            },
                            "stop_on_error": {
                                "type": "boolean",
                                "description": "Stop execution if any query fails",
                                "default": True
                            }
                        },
                        "required": ["queries"]
                    }
                )
            ]

        @self.call_tool()
        async def handle_operation(name: str, arguments: Dict[str, Any]):
            """
            Handle tool call requests by routing to specific methods.
            
            Args:
                name: Tool name
                arguments: Tool arguments
                
            Returns:
                List of TextContent objects with execution results
            """
            try:
                if name == "execute_query":
                    return await self.handle_execute_query(arguments)
                elif name == "inspect_schema":
                    return await self.handle_inspect_schema(arguments)
                elif name == "analyze_performance":
                    return await self.handle_analyze_performance(arguments)
                elif name == "check_data_quality":
                    return await self.handle_check_data_quality(arguments)
                elif name == "execute_batch":
                    return await self.handle_execute_batch(arguments)
                else:
                    return [TextContent(
                        type="text",
                        text=f"Unknown tool: {name}"
                    )]
            except Exception as e:
                error_message = f"Error in {name}: {str(e)}"
                logger.error(error_message)
                return [TextContent(
                    type="text",
                    text=error_message
                )]

    # Individual tool handlers
    async def handle_execute_query(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle basic query execution."""
        start_time = time.time()
        result = self.db.process_request(arguments["query"])
        execution_time = time.time() - start_time
        
        return [TextContent(
            type="text",
            text=f"Query executed successfully in {execution_time:.2f}s\n"
                 f"Rows returned: {len(result)}\n"
                 f"Results: {json.dumps(result, indent=2, default=str)}"
        )]

    async def handle_inspect_schema(self, arguments: Dict[str, Any]) -> List[TextContent]:
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

    async def handle_analyze_performance(self, arguments: Dict[str, Any]) -> List[TextContent]:
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
            logger.warning(f"Could not get query profile: {e}")
        
        return [TextContent(
            type="text",
            text="\n".join(analysis_results) if analysis_results else "No performance data available"
        )]

    async def handle_check_data_quality(self, arguments: Dict[str, Any]) -> List[TextContent]:
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

    async def handle_execute_batch(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle batch query execution with dependencies."""
        queries = arguments["queries"]
        stop_on_error = arguments.get("stop_on_error", True)
        
        results = {}
        executed = set()
        batch_results = []
        
        def can_execute(query_info):
            """Check if all dependencies are satisfied."""
            depends_on = query_info.get("depends_on", [])
            return all(dep in executed for dep in depends_on)
        
        # Execute queries respecting dependencies
        remaining_queries = queries.copy()
        
        while remaining_queries:
            executed_in_round = False
            
            for query_info in remaining_queries[:]:  # Create a copy to iterate
                if can_execute(query_info):
                    query_name = query_info["name"]
                    query_sql = query_info["query"]
                    
                    try:
                        start_time = time.time()
                        result = self.db.process_request(query_sql)
                        execution_time = time.time() - start_time
                        
                        results[query_name] = {
                            "success": True,
                            "result": result,
                            "execution_time": execution_time
                        }
                        executed.add(query_name)
                        remaining_queries.remove(query_info)
                        executed_in_round = True
                        
                        batch_results.append(f"✓ {query_name}: Completed in {execution_time:.2f}s")
                        
                    except Exception as e:
                        error_msg = f"✗ {query_name}: Failed - {str(e)}"
                        batch_results.append(error_msg)
                        results[query_name] = {
                            "success": False,
                            "error": str(e)
                        }
                        
                        if stop_on_error:
                            batch_results.append("Batch execution stopped due to error")
                            break
                        else:
                            executed.add(query_name)  # Mark as executed even if failed
                            remaining_queries.remove(query_info)
                            executed_in_round = True
            
            if not executed_in_round:
                # Circular dependency or missing dependency
                batch_results.append("Error: Circular dependency or missing dependency detected")
                break
        
        return [TextContent(
            type="text",
            text=f"Batch Execution Results:\n" + "\n".join(batch_results) + 
                 f"\n\nSummary: {len(executed)}/{len(queries)} queries executed"
        )]

    def __del__(self) -> None:
        """Clean up resources when the server is deleted."""
        if hasattr(self, 'db'):
            self.db.cleanup()


async def start_service() -> None:
    """Start and run the MCP server."""
    try:
        # Initialize the server
        server = SnowflakeServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting server")
        
        # Run the server using stdio communication
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                initialization_options
            )
    except Exception as e:
        logger.critical(f"Server failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Server shutting down")


if __name__ == "__main__":
    asyncio.run(start_service())