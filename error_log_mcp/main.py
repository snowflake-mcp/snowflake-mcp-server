import os
import json
import threading
from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime
from mcp.server import Server
from mcp.types import Tool, TextContent
import mcp.server.stdio
import traceback
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

# Remove JSON file reference
_LOCK = threading.Lock()

ALLOWED_ERROR_TYPES = {'error', 'warning', 'info', 'logical', 'other', 'failure'}

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('error_resolution_server')

load_dotenv()

class SnowflakeErrorResolutionDB:
    """Manages Snowflake database connections and query execution."""
    
    def __init__(self) -> None:
        """Initialize Snowflake connection configuration from environment variables."""
        self.config = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
             **({"password": os.getenv("SNOWFLAKE_PASSWORD")} if os.getenv("SNOWFLAKE_PASSWORD") else {"authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR")})
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
    
    def log_error(self, error_message: str, resolution: str, success: bool, note: Optional[str] = None, error_type: str = 'error') -> None:
        """Log an error and its resolution to Snowflake."""
        if error_type not in ALLOWED_ERROR_TYPES:
            error_type = 'other'
        
        with _LOCK:
            try:
                conn = self.verify_link()
                with conn as conn:
                    with conn.cursor(DictCursor) as cur:
                        # Check if this error_message + resolution combination exists
                        check_sql = """
                        SELECT id, success_count, failure_notes 
                        FROM error_log_db.error_log_schema.error_log_table 
                        WHERE error_message = %s AND resolution = %s
                        """
                        cur.execute(check_sql, (error_message, resolution))
                        existing = cur.fetchone()
                        
                        if existing:
                            # Update existing record
                            new_success_count = existing['success_count'] + (1 if success else 0)
                            
                            if success:
                                update_sql = """
                                UPDATE error_log_db.error_log_schema.error_log_table 
                                SET success_count = %s, error_type = %s, updated_at = CURRENT_TIMESTAMP()
                                WHERE id = %s
                                """
                                cur.execute(update_sql, (new_success_count, error_type, existing['id']))
                            else:
                                # Add failure note
                                existing_notes = existing.get('failure_notes') or ''
                                failure_note = note or "No note provided."
                                
                                if existing_notes:
                                    new_notes = existing_notes + " | " + failure_note
                                else:
                                    new_notes = failure_note
                                
                                update_sql = """
                                UPDATE error_log_db.error_log_schema.error_log_table 
                                SET failure_notes = %s, error_type = %s, updated_at = CURRENT_TIMESTAMP()
                                WHERE id = %s
                                """
                                cur.execute(update_sql, (new_notes, error_type, existing['id']))
                        else:
                            # Insert new record
                            failure_notes = None if success else (note or "No note provided.")
                            success_count = 1 if success else 0
                            
                            insert_sql = """
                            INSERT INTO error_log_db.error_log_schema.error_log_table 
                            (error_message, error_type, resolution, success_count, failure_notes)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                            cur.execute(insert_sql, (error_message, error_type, resolution, success_count, failure_notes))
                        
                        conn.commit()
                        logger.info(f"Successfully logged error: {error_message[:50]}...")
                        
            except Exception as e:
                logger.error(f"Failed to log error to Snowflake: {str(e)}")
                raise
    
    def get_resolutions(self, error_message: str) -> List[Dict[str, Any]]:
        """Get all resolutions for a specific error message, ordered by success count."""
        try:
            conn = self.verify_link()
            with conn as conn:
                with conn.cursor(DictCursor) as cur:
                    sql = """
                    SELECT resolution, success_count, failure_notes
                    FROM error_log_db.error_log_schema.error_log_table 
                    WHERE error_message = %s
                    ORDER BY success_count DESC
                    """
                    cur.execute(sql, (error_message,))
                    results = cur.fetchall()
                    
                    # Convert failure_notes string back to list format for compatibility
                    processed_results = []
                    for result in results:
                        failure_notes = result.get('failure_notes', '')
                        failure_notes_list = failure_notes.split(' | ') if failure_notes else []
                        
                        processed_results.append({
                            'resolution': result['resolution'],
                            'success_count': result['success_count'],
                            'failure_notes': failure_notes_list
                        })
                    
                    return processed_results
                    
        except Exception as e:
            logger.error(f"Failed to get resolutions from Snowflake: {str(e)}")
            return []
    
    def get_best_resolution(self, error_message: str) -> Optional[Dict[str, Any]]:
        """Get the best resolution (highest success count) for an error message."""
        resolutions = self.get_resolutions(error_message)
        return resolutions[0] if resolutions else None
    
    def get_error_type(self, error_message: str) -> Optional[str]:
        """Get the error type for a specific error message."""
        try:
            conn = self.verify_link()
            with conn as conn:
                with conn.cursor(DictCursor) as cur:
                    sql = """
                    SELECT error_type 
                    FROM error_log_db.error_log_schema.error_log_table 
                    WHERE error_message = %s 
                    LIMIT 1
                    """
                    cur.execute(sql, (error_message,))
                    result = cur.fetchone()
                    return result['error_type'] if result else None
                    
        except Exception as e:
            logger.error(f"Failed to get error type from Snowflake: {str(e)}")
            return None
    
    def get_all_errors(self) -> Dict[str, Any]:
        """Get all errors and their resolutions in a format similar to the JSON structure."""
        try:
            conn = self.verify_link()
            with conn as conn:
                with conn.cursor(DictCursor) as cur:
                    sql = """
                    SELECT error_message, error_type, resolution, success_count, failure_notes
                    FROM error_log_db.error_log_schema.error_log_table 
                    ORDER BY error_message, success_count DESC
                    """
                    cur.execute(sql)
                    results = cur.fetchall()
                    
                    # Group by error_message to maintain compatibility with original JSON structure
                    grouped_errors = {}
                    for result in results:
                        error_msg = result['error_message']
                        if error_msg not in grouped_errors:
                            grouped_errors[error_msg] = {
                                'error_type': result['error_type'],
                                'resolutions': []
                            }
                        
                        failure_notes = result.get('failure_notes', '')
                        failure_notes_list = failure_notes.split(' | ') if failure_notes else []
                        
                        grouped_errors[error_msg]['resolutions'].append({
                            'resolution': result['resolution'],
                            'success_count': result['success_count'],
                            'failure_notes': failure_notes_list
                        })
                    
                    return grouped_errors
                    
        except Exception as e:
            logger.error(f"Failed to get all errors from Snowflake: {str(e)}")
            return {}

class ErrorResolutionServer(Server):
    """MCP server for error resolution management."""
    def __init__(self) -> None:
        super().__init__(name="error-resolution-server")
        self.db = SnowflakeErrorResolutionDB()
        logger.info("ErrorResolutionServer initialized with Snowflake backend")

        @self.list_tools()
        async def get_supported_operations():
            return [
                Tool(
                    name="log_error",
                    description="Log an error, attempted resolution, and outcome to Snowflake.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "error_message": {"type": "string"},
                            "resolution": {"type": "string"},
                            "success": {"type": "boolean"},
                            "note": {"type": "string"},
                            "error_type": {"type": "string", "enum": list(ALLOWED_ERROR_TYPES)}
                        },
                        "required": ["error_message", "resolution", "success"]
                    }
                ),
                Tool(
                    name="get_resolutions",
                    description="Get all known resolutions for an error message from Snowflake.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "error_message": {"type": "string"}
                        },
                        "required": ["error_message"]
                    }
                ),
                Tool(
                    name="get_best_resolution",
                    description="Get the best-known resolution for an error message from Snowflake.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "error_message": {"type": "string"}
                        },
                        "required": ["error_message"]
                    }
                ),
                Tool(
                    name="get_error_type",
                    description="Get the error type for an error message from Snowflake.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "error_message": {"type": "string"}
                        },
                        "required": ["error_message"]
                    }
                ),
                Tool(
                    name="get_all_errors",
                    description="Get all logged errors and their resolutions from Snowflake.",
                    inputSchema={"type": "object", "properties": {}}
                )
            ]

        @self.call_tool()
        async def handle_operation(name: str, arguments: Dict[str, Any]):
            try:
                if name == "log_error":
                    self.db.log_error(
                        error_message=arguments["error_message"],
                        resolution=arguments["resolution"],
                        success=arguments["success"],
                        note=arguments.get("note"),
                        error_type=arguments.get("error_type", "error")
                    )
                    return [TextContent(type="text", text="Logged error and resolution to Snowflake.")]
                elif name == "get_resolutions":
                    result = self.db.get_resolutions(arguments["error_message"])
                    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
                elif name == "get_best_resolution":
                    result = self.db.get_best_resolution(arguments["error_message"])
                    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
                elif name == "get_error_type":
                    result = self.db.get_error_type(arguments["error_message"])
                    return [TextContent(type="text", text=str(result))]
                elif name == "get_all_errors":
                    result = self.db.get_all_errors()
                    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]
                
            except Exception as e:
                error_message = f"Error in {name}: {str(e)}"
                tb_str = traceback.format_exc()
                logger.error(error_message + "\n" + tb_str)
                return [TextContent(type="text", text=error_message + "\n" + tb_str)]

def __del__(self):
    if hasattr(self, 'db'):
        del self.db

async def start_service() -> None:
    try:
        server = ErrorResolutionServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting ErrorResolutionServer with Snowflake backend")
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
        logger.info("ErrorResolutionServer shutting down")

if __name__ == "__main__":
    asyncio.run(start_service())