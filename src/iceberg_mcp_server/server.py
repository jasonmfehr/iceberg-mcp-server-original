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
from dotenv import load_dotenv

from iceberg_mcp_server.mcp.tools.database import (
    build_get_schema_tool,
    build_use_db_tool,
    build_list_dbs_tool,
    build_resource_describe_db
)

from iceberg_mcp_server.tools.query import build_execute_query_tool
from iceberg_mcp_server.tools.impala_tools import close_conn


load_dotenv()

def setup_logging():
    log_dir = Path(os.getenv("LOG_DIR", "/tmp/iceberg-mcp-server"))
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "iceberg_mcp_server.log"

    log_level_name = os.getenv("LOG_LEVEL", "INFO")
    log_level = getattr(logging, log_level_name.upper(), logging.INFO)

    logging.basicConfig(
        filename=str(log_file),
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s:%(module)s/%(filename)s:%(lineno)d: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Set specific log levels for impala loggers.
    impala_log_level_name = os.getenv("IMPALA_LOG_LEVEL", "INFO")
    impala_log_level = getattr(logging, impala_log_level_name.upper(), logging.INFO)
    logging.getLogger("impala").setLevel(logging.INFO)

    # Set specific log levels for FastMCP loggers.
    mcp_log_level_name = os.getenv("MCP_LOG_LEVEL", "INFO")
    mcp_log_level = getattr(logging, mcp_log_level_name.upper(), logging.INFO)
    logging.getLogger("mcp").setLevel(mcp_log_level)

logger = logging.getLogger("iceberg-mcp-server")


def build_mcp_server() -> FastMCP:
    mcp = FastMCP(
        name="Cloudera Iceberg MCP Server via Impala",
        tools=[
            build_execute_query_tool(),
            build_get_schema_tool(),
            build_use_db_tool(),
            build_list_dbs_tool(),]
    )

    mcp.add_resource(build_resource_describe_db())
    return mcp

def main():
    setup_logging()
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    logger.info(f"Starting Iceberg MCP Server via transport: {transport}")
    mcp = build_mcp_server()

    try:
        mcp.run(transport=transport)
    except Exception as e:
        logger.critical(f"MCP server crashed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.info("Initializing Iceberg MCP Server")
    main()
    close_conn()
    logger.info("Shutting down Iceberg MCP Server")
