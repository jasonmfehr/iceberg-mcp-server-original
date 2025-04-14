from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
load_dotenv()

from tools import impala_tools

mcp = FastMCP(name="Cloudera Iceberg MCP Server via Impala")

# Register functions as MCP tools
@mcp.tool()
def execute_query(query: str) -> str:
    return impala_tools.execute_query(query)

@mcp.tool()
def get_schema() -> str:
    return impala_tools.get_schema()

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
