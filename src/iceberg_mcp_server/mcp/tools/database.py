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
from fastmcp.resources.template import FunctionResourceTemplate 
from iceberg_mcp_server.tools import impala_tools

__all__ = ["build_get_schema_tool", "build_use_db_tool", "build_list_dbs_tool", "build_resource_describe_db"]

logger = logging.getLogger("iceberg-mcp-server")

def _get_schema() -> str:
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
        fn=_get_schema,
        name="get_schema",
        description="Retrieve the list of table names in the current Impala database.",
    )

def _use_db(db_name: Annotated[str, "name of a valid database that will be used for all requests where a database is not provided"]) -> str:
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
        fn=_use_db,
        name="use_db",
        title="Use Database",
        description="Use the specified database in Impala.",
    )

def _list_dbs() -> list[str]:
    """
    List all databases in Impala.
    """
    global logger
    logger.info("Listing databases")
    try:
        result = impala_tools.list_dbs()
        logger.debug(f"list_dbs result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        raise

def build_list_dbs_tool() -> Tool:
    global logger
    logger.debug("Building list_dbs tool")

    return Tool.from_function(
        fn=_list_dbs,
        name="list_dbs",
        description="List all databases in Impala.",
    )


def _resource_describe_db(db_name: str) -> str:
    """
    Describe the specified database in Impala.
    """
    import pdb; pdb.set_trace()
    global logger
    logger.info(f"Describing database: {db_name}")
    ret_str = ""

    try:
        result = impala_tools.execute_query(f"DESCRIBE DATABASE {db_name}")
        logger.debug(f"resource_describe_db result: {result}")
        ret_str += f"Database: {db_name}\n"
        ret_str += f"Storage Location: {result[0][1]}\n"
        ret_str += f"Comment: {result[0][2]}\n"
        return ret_str
    except Exception as e:
        logger.error(f"Error in resource_describe_db: {e}")
        raise


def build_resource_describe_db() -> FunctionResourceTemplate:
    global logger
    logger.debug("Building database resource")

    return FunctionResourceTemplate.from_function(
        uri_template="database://{db_name}",
        name="Describe Database",
        description="Resource that provides detailed information about a specific database.",
        mime_type="text/plain",
        annotations={
            "readOnlyHint": True,
            "idempotentHint": True
        },
        fn=_resource_describe_db,
    )