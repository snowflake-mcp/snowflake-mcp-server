import os
import logging
import json
from typing import Optional, Any, Dict, List
from req_handler.process_requrest import ProcessReq
from req_handler.list_databases import ListDatabases
from req_handler.list_schemas import ListSchemas
from req_handler.list_tables import ListTables
from req_handler.describe_table import DescribeTables
from req_handler.get_table_sample import GetTableSample
from req_handler.get_column_stats import GetColumnStats
from req_handler.search_tables import SearchTables
from req_handler.search_columns import SearchColumns
from req_handler.get_warehouse_info import GetWarehouseInfo
# from req_handler.inspect_schema import InspectSchema
from req_handler.check_data_quality import CheckDataQuality
from req_handler.analyze_performance import AnalyzePerformance
from req_handler.create_stored_procedure import CreateStoredProcedure
import snowflake.connector
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger('snowflake_connection')

# Load environment variables
load_dotenv()


class SnowflakeConnection(ProcessReq,ListDatabases,ListSchemas,ListTables,DescribeTables,GetTableSample,GetColumnStats,SearchTables,SearchColumns,GetWarehouseInfo,CheckDataQuality,AnalyzePerformance,CreateStoredProcedure):
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
