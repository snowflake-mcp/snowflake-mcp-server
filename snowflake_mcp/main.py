#!/usr/bin/env python
"""
Enhanced Snowflake Model Context Protocol (MCP) Server

This server enables Claude to execute SQL queries and discover metadata on Snowflake databases 
through the Model Context Protocol (MCP). It includes comprehensive metadata discovery tools
for data exploration and analysis.
"""
import os
import asyncio
import logging
import json
import time
from typing import Optional, Any, Dict, List, Tuple

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
            "role": "ACCOUNTADMIN"
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
                    logger.error(f"Error executing statement: {stmt}\n{str(e)}")
                    raise

        execution_time = time.time() - start_time
        logger.info(f"Executed {len(statements)} statements in {execution_time:.2f}s")
        return results
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases accessible to the current user."""
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
    def list_schemas(self, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all schemas in a database."""
        conn = self.verify_link()
        with conn.cursor() as cursor:
            if database_name:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {database_name}")
            else:
                cursor.execute("SHOW SCHEMAS")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
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
    
    def describe_table(self, table_name: str, database_name: Optional[str] = None, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        # Build fully qualified table name
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        parts.append(table_name)
        
        full_table_name = '.'.join(parts)
        
        # Get column information
        columns_query = f"""
        SELECT 
            ordinal_position,
            column_name,
            data_type,
            is_nullable,
            column_default,
            is_identity,
            comment,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        
        if database_name:
            columns_query += f" AND table_catalog = '{database_name}'"
        if schema_name:
            columns_query += f" AND table_schema = '{schema_name}'"
            
        columns_query += " ORDER BY ordinal_position"
        
        # Get table metadata
        table_query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            table_type,
            created,
            last_altered,
            comment,
            row_count,
            bytes,
            clustering_key,
            auto_clustering_on
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """
        
        if database_name:
            table_query += f" AND table_catalog = '{database_name}'"
        if schema_name:
            table_query += f" AND table_schema = '{schema_name}'"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            # Get table metadata
            cursor.execute(table_query)
            table_columns = [col[0] for col in cursor.description]
            table_rows = cursor.fetchall()
            table_info = dict(zip(table_columns, table_rows[0])) if table_rows else {}
            
            # Get column information
            cursor.execute(columns_query)
            col_columns = [col[0] for col in cursor.description]
            col_rows = cursor.fetchall()
            columns_info = [dict(zip(col_columns, row)) for row in col_rows]
            
        return {
            "table_info": table_info,
            "columns": columns_info,
            "column_count": len(columns_info)
        }
    
    def get_table_sample(self, table_name: str, database_name: Optional[str] = None, 
                        schema_name: Optional[str] = None, limit: int = 10) -> Dict[str, Any]:
        """Get a sample of data from a table."""
        # Build fully qualified table name
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        parts.append(table_name)
        
        full_table_name = '.'.join(parts)
        
        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
        return {
            "table_name": full_table_name,
            "columns": columns,
            "sample_data": [dict(zip(columns, row)) for row in rows],
            "sample_size": len(rows)
        }
    
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
                logger.error(f"Error getting column stats: {str(e)}")
                raise
    
    def search_tables(self, search_term: str, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for tables by name or comment."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            table_type,
            comment,
            row_count,
            bytes
        FROM information_schema.tables
        WHERE (UPPER(table_name) LIKE UPPER('%{search_term}%')
           OR UPPER(comment) LIKE UPPER('%{search_term}%'))
        """
        
        if database_name:
            query += f" AND table_catalog = '{database_name}'"
            
        query += " ORDER BY table_catalog, table_schema, table_name"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
    def search_columns(self, search_term: str, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for columns by name or comment."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            column_name,
            data_type,
            comment
        FROM information_schema.columns
        WHERE (UPPER(column_name) LIKE UPPER('%{search_term}%')
           OR UPPER(comment) LIKE UPPER('%{search_term}%'))
        """
        
        if database_name:
            query += f" AND table_catalog = '{database_name}'"
            
        query += " ORDER BY table_catalog, table_schema, table_name, ordinal_position"
        
        conn = self.verify_link()
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
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
                    logger.warning(f"Could not fetch usage data for {warehouse_name}: {str(e)}")
                
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
                    logger.warning(f"Could not fetch load data for {warehouse_name}: {str(e)}")
                
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
                        "required": []
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
                            }
                        },
                        "required": []
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
                        "required": ["table_name"]
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
                        "required": ["table_name"]
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
                        "required": ["table_name", "column_name"]
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
                        "required": ["search_term"]
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
                        "required": ["search_term"]
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
                )
            ]

        @self.call_tool()
        async def handle_operation(name: str, arguments: Dict[str, Any]):
            """
            Handle tool call requests.
            
            Args:
                name: Tool name
                arguments: Tool arguments
                
            Returns:
                List of TextContent objects with execution results
            """
            start_time = time.time()
            
            try:
                if name == "execute_query":
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


async def start_service() -> None:
    """Start and run the MCP server."""
    try:
        # Initialize the server
        server = SnowflakeServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting enhanced server with metadata discovery")
        
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