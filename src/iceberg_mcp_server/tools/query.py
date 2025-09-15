## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import json
import logging

from fastmcp.tools import Tool
from iceberg_mcp_server.tools import impala_tools

logger = logging.getLogger("iceberg-mcp-server")


def execute_query(query: str) -> str:
    """
    Execute a SQL query on the Impala database and return results as JSON.
    """
    logger.info(f"Executing query: {query}")
    try:
        result = json.dumps(impala_tools.execute_query(query), default=str)
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
