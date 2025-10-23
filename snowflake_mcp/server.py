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
from mcp import types
from pydantic import AnyUrl
from connection import SnowflakeConnection

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('snowflake_server')

# Load environment variables
load_dotenv()


class SnowflakeServer(Server):
    """MCP server that handles Snowflake database operations with metadata discovery."""
    
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
                    name="process_req",
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
                        },
                        "required": ["table_name", "schema_name"]
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
                            }
                        },
                        "required": ["table_name"]
                    }
                ),
                Tool(
                    name="list_databases",
                    description="List all databases accessible to the current user",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="list_schemas",
                    description="List all schemas in a database",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "database_name": {
                                "type": "string",
                                "description": "Database name (optional - if not provided, lists schemas from all databases)"
                            }
                        },
                        "required": ["database_name"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in a database/schema",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "database_name": {
                                "type": "string",
                                "description": "Database name (optional)"
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
                        "required": ["database_name", "schema_name"]
                    }
                ),
                Tool(
                    name="describe_table",
                    description="Get detailed information about a specific table including columns and metadata",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to describe"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name (optional)"
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)"
                            }
                        },
                        "required": ["table_name", "database_name", "schema_name"]
                    }
                ),
                Tool(
                    name="get_table_sample",
                    description="Get a sample of data from a table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to sample"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name (optional)"
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Number of rows to sample (default: 10, max: 100)",
                                "minimum": 1,
                                "maximum": 100
                            }
                        },
                        "required": ["table_name", "database_name", "schema_name"]
                    }
                ),
                Tool(
                    name="get_column_stats",
                    description="Get statistical information about a specific column",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table"
                            },
                            "column_name": {
                                "type": "string",
                                "description": "Name of the column"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name (optional)"
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)"
                            }
                        },
                        "required": ["table_name", "column_name", "database_name", "schema_name"]
                    }
                ),
                Tool(
                    name="search_tables",
                    description="Search for tables by name or comment",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "search_term": {
                                "type": "string",
                                "description": "Term to search for in table names and comments"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name to limit search (optional)"
                            }
                        },
                        "required": ["search_term", "database_name"]
                    }
                ),
                Tool(
                    name="search_columns",
                    description="Search for columns by name or comment",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "search_term": {
                                "type": "string",
                                "description": "Term to search for in column names and comments"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name to limit search (optional)"
                            }
                        },
                        "required": ["search_term", "database_name"]
                    }
                ),
                Tool(
                    name="get_warehouse_info",
                    description="Get comprehensive information about available warehouses including usage statistics and performance metrics",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="drop_database",
                    description="Drop a database from Snowflake",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "database_name": {
                                "type": "string",
                                "description": "Name of the database to drop"
                            }
                        },
                        "required": ["database_name"]
                    }
                ),
                Tool(
                    name="create_stored_procedure",
                    description="Create a stored procedure in Snowflake from a .sql file",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql_file_path": {
                                "type": "string",
                                "description": "Path to the .sql file containing the stored procedure definition"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name to create the procedure in (optional)"
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name to create the procedure in (optional)"
                            },
                            "replace_if_exists": {
                                "type": "boolean",
                                "description": "Replace procedure if it already exists (default: true)",
                                "default": True
                            }
                        },
                        "required": ["sql_file_path"]
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
                
            start_time = time.time()
            try:
                if name == "process_req":
                    result = self.db.process_request(arguments["query"])
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Query Results (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "list_databases":
                    result = self.db.list_databases()
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Databases (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "list_schemas":
                    database_name = arguments.get("database_name")
                    result = self.db.list_schemas(database_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Schemas (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "list_tables":
                    database_name = arguments.get("database_name")
                    schema_name = arguments.get("schema_name")
                    result = self.db.list_tables(database_name, schema_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Tables (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "describe_table":
                    table_name = arguments["table_name"]
                    database_name = arguments.get("database_name")
                    schema_name = arguments.get("schema_name")
                    result = self.db.describe_table(table_name, database_name, schema_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Table Description (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "get_table_sample":
                    table_name = arguments["table_name"]
                    database_name = arguments.get("database_name")
                    schema_name = arguments.get("schema_name")
                    limit = min(arguments.get("limit", 10), 100)  # Cap at 100 rows
                    result = self.db.get_table_sample(table_name, database_name, schema_name, limit)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Table Sample (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "get_column_stats":
                    table_name = arguments["table_name"]
                    column_name = arguments["column_name"]
                    database_name = arguments.get("database_name")
                    schema_name = arguments.get("schema_name")
                    result = self.db.get_column_stats(table_name, column_name, database_name, schema_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Column Statistics (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "search_tables":
                    search_term = arguments["search_term"]
                    database_name = arguments.get("database_name")
                    result = self.db.search_tables(search_term, database_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Table Search Results (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "search_columns":
                    search_term = arguments["search_term"]
                    database_name = arguments.get("database_name")
                    result = self.db.search_columns(search_term, database_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Column Search Results (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "get_warehouse_info":
                    result = self.db.get_warehouse_info()
                    execution_time = time.time() - start_time
                    
                    # Convert datetime objects to strings to ensure JSON serialization works
                    try:
                        result_str = json.dumps(result, indent=2, default=str)
                    except Exception as json_error:
                        logger.error(f"JSON serialization error: {str(json_error)}")
                        result_str = str(result)
                    
                    return [TextContent(
                        type="text",
                        text=f"Warehouse Information (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                elif name == "drop_database":
                    database_name = arguments["database_name"]
                    result = self.db.drop_database(database_name)
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    return [TextContent(
                        type="text",
                        text=f"Database Dropped (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                elif name == "create_stored_procedure":
                    sql_file_path = arguments["sql_file_path"]
                    database_name = arguments.get("database_name")
                    schema_name = arguments.get("schema_name")
                    replace_if_exists = arguments.get("replace_if_exists", True)
                    
                    result = self.db.create_stored_procedure_from_file(
                        sql_file_path, database_name, schema_name, replace_if_exists
                    )
                    execution_time = time.time() - start_time
                    result_str = json.dumps(result, indent=2, default=str)
                    
                    status = "SUCCESS" if result.get("success") else "FAILED"
                    return [TextContent(
                        type="text",
                        text=f"Stored Procedure Creation {status} (execution time: {execution_time:.2f}s):\n{result_str}"
                    )]
                
                elif name == "inspect_schema":
                    return await self.handle_inspect_schema(arguments)
                elif name == "analyze_performance":
                    return await self.handle_analyze_performance(arguments)
                elif name == "check_data_quality":
                    return await self.handle_check_data_quality(arguments)
                
                else:
                    return [TextContent(
                        type="text",
                        text=f"Unknown tool: {name}"
                    )]
                    
            except Exception as e:
                execution_time = time.time() - start_time
                error_message = f"Error executing {name}: {str(e)} (execution time: {execution_time:.2f}s)"
                logger.error(error_message)
                return [TextContent(
                    type="text",
                    text=error_message
                )]

    def __del__(self) -> None:
        """Clean up resources when the server is deleted."""
        if hasattr(self, 'db'):
            self.db.cleanup()
