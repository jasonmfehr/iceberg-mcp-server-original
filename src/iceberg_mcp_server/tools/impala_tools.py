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
import os
from impala.dbapi import connect

conn = None
logger = logging.getLogger("iceberg-mcp-server")

# Helper to get Impala connection details from env vars
def get_db_connection():
    host = os.getenv("IMPALA_HOST", "coordinator-default-impala.example.com")
    port = int(os.getenv("IMPALA_PORT", "443"))
    user = os.getenv("IMPALA_USER", "")
    password = os.getenv("IMPALA_PASSWORD", "")
    database = os.getenv("IMPALA_DATABASE", "default")
    auth_mechanism = os.getenv("IMPALA_AUTH_MECHANISM", "LDAP")
    use_http_transport = os.getenv("IMPALA_USE_HTTP_TRANSPORT", "true")
    http_path = os.getenv("IMPALA_HTTP_PATH", "cliservice")
    use_ssl = os.getenv("IMPALA_USE_SSL", "true")

    if use_http_transport.lower() == "true":
        use_http_transport = True
    else:
        use_http_transport = False

    if use_ssl.lower() == "true":
        use_ssl = True
    else:
        use_ssl = False

    logger.debug(f"Establishing new Impala connection to {host}:{port}, user={user}, "
        f"db={database}, auth={auth_mechanism}, http={use_http_transport}, ssl={use_ssl}")

    if user == "":
        user = None

    if password == "":
        password = None

    return connect(
        host=host,
        port=port,
        user=user,
        password=password,
        auth_mechanism=auth_mechanism,
        use_http_transport=use_http_transport,
        http_path=http_path,
        use_ssl=use_ssl,
    )


def close_conn():
    global conn
    if conn is not None:
        logger.debug("Closing Impala connection")
        try:
            conn.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        finally:
          conn = None
    else:
        logger.debug("No Impala connection to close")


def switch_db(db_name: str) -> str:
    global conn

    logger.debug(f"Enter switch_db - Switching to database: '{db_name}'")

    if conn is None:
        try:
            conn = get_db_connection()
        except Exception as e:
            return f"Error: {str(e)}"
    else:
        conn.default_db = db_name

    try:
        cur = conn.cursor()
        cur.execute(f"use {db_name}")
        cur.close()
        return f"Switched to database {db_name}"
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        logger.debug(f"Exit switch_db")


def execute_query(query: str) -> list:
    global conn

    logger.debug(f"Enter execute_query - Received query: {query}")

    # Implement rudimentary SQL injection prevention
    # In this case, we only allow read-only queries
    # This is a very basic check and should be improved for production use
    readonly_prefixes = ["select", "show", "describe", "with", "use", "set"]

    if not query.strip().lower().split()[0] in readonly_prefixes:
        return "Only read-only queries are allowed."

    if conn is None:
        try:
            conn = get_db_connection()
        except Exception as e:
            logger.debug(f"Exit execute_query - Failed to establish connection")
            return f"Error: {str(e)}"

    try:
        cur = conn.cursor()
        cur.execute(query)
        if cur.description:
            return cur.fetchall()
        else:
            conn.commit()
            result = "Query executed successfully."
        cur.close()
        return result
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        logger.debug(f"Exit execute_query")


def get_schema() -> str:
    global conn

    logger.debug(f"Enter get_schema")
    if conn is None:
        try:
            conn = get_db_connection()
        except Exception as e:
            logger.debug(f"Exit get_schema - Failed to establish connection")
            return f"Error: {str(e)}"

    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        schema = [table[0] for table in tables]
        return json.dumps(schema)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        logger.debug(f"Exit get_schema")


def list_dbs() -> list[str]:
    global conn

    logger.debug(f"Enter list_dbs")
    if conn is None:
        try:
            conn = get_db_connection()
        except Exception as e:
            logger.debug(f"Exit list_dbs - Failed to establish connection")
            return [f"Error: {str(e)}"]

    try:
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        dbs = cur.fetchall()
        db_list = [db[0] for db in dbs]
        return db_list
    except Exception as e:
        return [f"Error: {str(e)}"]
    finally:
        logger.debug(f"Exit list_dbs")
