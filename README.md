# Snowflake MCP Server

`snowflake-mcp-server` : This server acts as bridge between your AI application and snowflake warehouse. This sever consist of various tools which will help you interact with the warehouse and perform operation seamlessly with minimal configuration.

---
## MCP server List
** Snowflake MCP server
** Error_log MCP server 
-- for both the above server need to write the use case and advantages of using it.


## What can be achived by using this tool
* Reduce the time between switching between your code IDE and Snowflake iterface
* Chat based database intereact 
* This mcp server consist of in total 5 tools
** execute_query: This tool executes query on snowflake
** inspect_schema: This tool will inspect the table and return the description of the table.
** Analyse the performance: This tool can be used to analyse the performace of the query 
** Check_data_quality: This tool will check the data quality in the table
** execute_batch: this toll will handel the multiple query run which is not handled by execute_query

*** This mcp server can handle both password based authentication and SSO authentication.

## 1. What is the MCP Server?
Model Context Protocol is an open standard that defines how models exchange, manage, and reason over shared context.


## 2. How MCP Servers Help Data Engineers
* MCP servers enable data engineers to connect AI models directly with enterprise data systems, streamlining access, automation, and insights.
* By providing standardized, secure connectivity between data warehouses and AI tools, MCP servers simplify integration and improve productivity.
* Data engineers can leverage MCP to automate data workflows, enhance context for AI applications, and reduce manual integration overhead.
* With MCP, data engineers spend less time managing connections and more time focusing on data quality, modeling, and innovation.

## MCP architecture
It consist of three main part 
1. MCP Host
2. MCP Client
3. MCP Server

## 5. MCP Config Section

The server config is for CURSOR IDE
Make sure you create env and install the required dependencies 
Below is an example of the configuration structure.
```
{
  "mcpServers": {
    "snowflake": {
      "command": "~/venv/bin/python3.13", 
      "args": ["path to main.py"]
    }
  }
}
```

1. Clone the repo
2. create a venv 
```
python -m venv venv
```
3. Activate the venv
```
#for mac
source venv/bin/activate
#for windows
venv/Scripts/activate
```
4. Install all the dependencies present in requirements.txt
```
pip install -r requirements.txt
```
5. Setup the mcp server
* go to cursor settings -> mcp tool --> add server
6. Add the following JSON 
```
{
  "mcpServers": {
    "error_log": {
      "command": "yourPath/snowflake_mcp_server/venv/bin/python3.13",
      "args": ["yourPath/snowflake-mcp-server/error_log_mcp/main.py"]
    }
  }
}
```
7. Change the path to point to the main.py and python in venv/bin or venv/scripts










