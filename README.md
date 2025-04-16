# Cloudera Iceberg MCP Server (via Impala)

This is a A Model Context Protocol server that provides read-only access to Iceberg tables via Apache Impala. This server enables LLMs to inspect database schemas and execute read-only queries.

- `execute_query(query: str)`: Run any SQL query on Impala and return the results as JSON.
- `get_schema()`: List all tables available in the current database.

## Usage with Claude Desktop

To use this server with the Claude Desktop app, add the following configuration to the "mcpServers" section of your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "iceberg-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/iceberg-mcp-server",
        "run",
        "server.py"
      ],
      "env": {
        "IMPALA_HOST": "coordinator-default-impala.example.com",
        "IMPALA_PORT": "443",
        "IMPALA_USER": "username",
        "IMPALA_PASSWORD": "password",
        "IMPALA_DATABASE": "default"
      }
    }
  }
}
```

Replace `/path/to` with your path to this repository and set the environment variables.

## Usage with AI frameworks

The `./examples` folder contains several examples how to integrate this MCP Server with common AI Frameworks like LangChain/LangGraph, OpenAI SDK. 
