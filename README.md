# Snowflake MCP Server

`snowflake-mcp-server` (Master Control Program Server) is a centralized application designed to manage, orchestrate, and monitor data engineering workflows and configurations for Snowflake. It acts as a control plane, providing data engineers with a unified interface to automate and standardize their data pipelines.

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


## 2. How This Helps Data Engineers

Implementing the Snowflake MCP Server empowers data engineering teams in several key ways:


## 5. MCP Config Section

The server config is for CURSOR IDE

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

## Task to be done
* Key-value pair authentication
* Store data in csv when asked to 
* error_loging tool which logs the error into csv file and 
* error_fix tool which fix the errors by checking if that common error occured before.



