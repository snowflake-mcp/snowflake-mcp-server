"""
Snowflake Model Context Protocol (MCP) Server

This server enables Claude to execute SQL queries on Snowflake databases through
the Model Context Protocol (MCP). It handles connection lifecycle management,
query execution, and result formatting.
"""
import asyncio
import logging
import mcp.server.stdio #this line is req in main.py
from server import SnowflakeServer

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('snowflake_server')

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