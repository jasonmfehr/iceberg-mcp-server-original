## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import logging

from typing import Annotated

from fastmcp.tools import Tool
from iceberg_mcp_server.tools import impala_tools

logger = logging.getLogger("iceberg-mcp-server")

def get_schema() -> str:
    """
    Retrieve the list of table names in the current Impala database.
    """
    global logger
    logger.info("Getting schema information")
    try:
        result = impala_tools.get_schema()
        logger.debug(f"Schema result: {result[:100]}...")
        return result
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise

def build_get_schema_tool() -> Tool:
    global logger
    logger.debug("Building get_schema tool")
    return Tool.from_function(
        fn=get_schema,
        name="get_schema",
        description="Retrieve the list of table names in the current Impala database.",
    )

def use_db(db_name: Annotated[str, "name of a valid database that will be used for all requests where a database is not provided"]) -> str:
    """
    Switch to the specified database in Impala.
    """
    global logger
    logger.info(f"Switching to database: {db_name}")
    try:
        result = impala_tools.switch_db(db_name)
        logger.debug(f"use_db result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error switching database: {e}")
        raise

def build_use_db_tool() -> Tool:
    global logger
    logger.debug("Building use_db tool")
    return Tool.from_function(
        fn=use_db,
        name="use_db",
        title="Use Database",
        description="Use the specified database in Impala.",
    )