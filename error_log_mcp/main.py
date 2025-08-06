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

ERRORS_FILE = '/Users/hithesh.patel/Desktop/mcp_proj_repo/snowflake-mcp-server/error_log_mcp/error_resolutions.json'
_LOCK = threading.Lock()

ALLOWED_ERROR_TYPES = {'error', 'warning', 'info', 'logical', 'other', 'failure'}

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('error_resolution_server')

class ErrorResolutionDB:
    """Handles error resolution data storage and retrieval."""
    def _load_data(self) -> Dict[str, Any]:
        if not os.path.exists(ERRORS_FILE):
            return {}
        with open(ERRORS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except Exception:
                return {}

    def _save_data(self, data: Dict[str, Any]) -> None:
        # Fix: Use 'w' mode to create/overwrite file properly
        with open(ERRORS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def log_error(self, error_message: str, resolution: str, success: bool, note: Optional[str] = None, error_type: str = 'error') -> None:
        if error_type not in ALLOWED_ERROR_TYPES:
            error_type = 'other'
        with _LOCK:
            data = self._load_data()
            entry = data.setdefault(error_message, {"resolutions": [], "error_type": error_type})
            if entry.get("error_type") != error_type:
                entry["error_type"] = error_type
            resolutions = entry["resolutions"]
            for res in resolutions:
                if res["resolution"] == resolution:
                    if success:
                        res["success_count"] += 1
                    else:
                        res.setdefault("failure_notes", []).append(note or "No note provided.")
                    break
            else:
                resolutions.append({
                    "resolution": resolution,
                    "success_count": 1 if success else 0,
                    "failure_notes": [] if success else [note or "No note provided."]
                })
            resolutions.sort(key=lambda r: r["success_count"], reverse=True)
            self._save_data(data)

    def get_resolutions(self, error_message: str) -> List[Dict[str, Any]]:
        with _LOCK:
            data = self._load_data()
            entry = data.get(error_message)
            if not entry:
                return []
            return entry["resolutions"]

    def get_best_resolution(self, error_message: str) -> Optional[Dict[str, Any]]:
        resolutions = self.get_resolutions(error_message)
        return resolutions[0] if resolutions else None

    def get_error_type(self, error_message: str) -> Optional[str]:
        with _LOCK:
            data = self._load_data()
            entry = data.get(error_message)
            if not entry:
                return None
            return entry.get("error_type")

    def get_all_errors(self) -> Dict[str, Any]:
        with _LOCK:
            return self._load_data()

class ErrorResolutionServer(Server):
    """MCP server for error resolution management."""
    def __init__(self) -> None:
        super().__init__(name="error-resolution-server")
        self.db = ErrorResolutionDB()
        logger.info("ErrorResolutionServer initialized")

        @self.list_tools()
        async def get_supported_operations():
            return [
                Tool(
                    name="log_error",
                    description="Log an error, attempted resolution, and outcome.",
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
                    description="Get all known resolutions for an error message.",
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
                    description="Get the best-known resolution for an error message.",
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
                    description="Get the error type for an error message.",
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
                    description="Get all logged errors and their resolutions.",
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
                    return [TextContent(type="text", text="Logged error and resolution.")]
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
        logger.info("Starting ErrorResolutionServer")
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