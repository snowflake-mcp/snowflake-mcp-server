import os
import logging
import json
from typing import Optional, Any, Dict, List
from tools.ProcessRequest import ProcessReq
from tools.ListDatabases import ListDatabases
from tools.ListSchemas import ListSchemas
from tools.ListTables import ListTables
from tools.DescribeTable import DescribeTables
from tools.GetTableSampleData import GetTableSample
from tools.GetColumnStats import GetColumnStats
from tools.SearchTables import SearchTables
from tools.SearchColumns import SearchColumns
from tools.GetWarehouseInfo import GetWarehouseInfo
from tools.InspectSchema import InspectSchema
from tools.CheckDataQuality import CheckDataQuality
from tools.AnalyzePerformance import AnalyzePerformance
from tools.CreateStoredProcedure import CreateStoredProcedure
import snowflake.connector
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger('snowflake_connection')

# Load environment variables
load_dotenv()
#hre
class SnowflakeConnection(ProcessReq,ListDatabases,ListSchemas,ListTables,DescribeTables,GetTableSample,GetColumnStats,SearchTables,SearchColumns,GetWarehouseInfo,InspectSchema,CheckDataQuality,AnalyzePerformance,CreateStoredProcedure):
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
