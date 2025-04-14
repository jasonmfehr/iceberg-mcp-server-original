from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
load_dotenv()

mcp = FastMCP("Cloudera Iceberg MCP Server via Impala")

import tools.impala_tools

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
