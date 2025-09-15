import asyncio
import os

from fastmcp import Client, FastMCP
from dotenv import load_dotenv

load_dotenv()

from iceberg_mcp_server import server

config = {
    "mcpServers": {
      "iceberg_mcp_server": {
        "transport": "stdio",
        "env": {
          "IMPALA_HOST": "localhost",
          "IMPALA_PORT": "28000",
          "IMPALA_AUTH_MECHANISM": "PLAIN",
          "IMPALA_USE_SSL": "false",
          "IMPALA_HTTP_PATH": "",
          "IMPALA_USER": "",
          "IMPALA_PASSWORD": "",
        },
        "command": "uv",
        "args": [
          "--directory",
          "/Users/jfehr/dev/jfehr/iceberg-mcp-server-original",
          "run",
          "src/iceberg_mcp_server/server.py"
        ],
      }
    }
}

os.environ["IMPALA_HOST"] = "localhost"
os.environ["IMPALA_PORT"] = "28000"
os.environ["IMPALA_AUTH_MECHANISM"] = "NOSASL"
os.environ["IMPALA_USE_SSL"] = "false"
os.environ["LOG_DIR"] = os.path.dirname(os.path.abspath(__file__))
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["IMPALA_LOG_LEVEL"] = "INFO"
os.environ["MCP_LOG_LEVEL"] = "DEBUG"

server.setup_logging()
mcp = server.build_mcp_server()
client = Client(mcp)

async def main():
  async with client:
    await client.ping()

    # Now try to read the resource
    result = await client.read_resource("database://functional")
    print(result)

asyncio.run(main())