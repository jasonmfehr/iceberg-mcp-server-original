## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import os
import logging
from pathlib import Path
from fastmcp import FastMCP
from fastmcp.tools import Tool
from dotenv import load_dotenv

load_dotenv()

from iceberg_mcp_server.tools import impala_tools

# Set up logging
log_dir = Path(os.getenv("LOG_DIR", "/tmp/iceberg-mcp-server"))
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "iceberg_mcp_server.log"

log_level_name = os.getenv("LOG_LEVEL", "INFO")
log_level = getattr(logging, log_level_name.upper(), logging.INFO)

logging.basicConfig(
    filename=str(log_file),
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S.%f"
)

logger = logging.getLogger("iceberg-mcp-server")

# Register functions as MCP tools
# @mcp.tool()
def execute_query(query: str) -> str:
    """
    Execute a SQL query on the Impala database and return results as JSON.
    """
    logger.info(f"Executing query: {query}")
    try:
        result = impala_tools.execute_query(query)
        logger.debug(f"Query result: {result[:100]}...")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

def build_execute_query_tool() -> Tool:
    logger.debug("Building execute_query tool")
    return Tool.from_function(
    fn=execute_query,
    name="execute_query",
    description="Execute a SQL query on the Impala database and return results as JSON.",
    )

# @mcp.tool()
def get_schema() -> str:
    """
    Retrieve the list of table names in the current Impala database.
    """
    logger.info("Getting schema information")
    try:
        result = impala_tools.get_schema()
        logger.debug(f"Schema result: {result[:100]}...")
        return result
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise

def build_get_schema_tool() -> Tool:
    logger.debug("Building get_schema tool")
    return Tool.from_function(
        fn=get_schema,
        name="get_schema",
        description="Retrieve the list of table names in the current Impala database.",
    )


def main():
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    logger.info(f"Starting Iceberg MCP Server via transport: {transport}")
    mcp = FastMCP(
        name="Cloudera Iceberg MCP Server via Impala",
        tools=[build_execute_query_tool(), build_get_schema_tool()]
    )

    try:
        mcp.run(transport=transport)
    except Exception as e:
        logger.critical(f"MCP server crashed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.info("Initializing Iceberg MCP Server")
    main()
