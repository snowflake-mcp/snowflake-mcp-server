import os
import re
import time
from typing import Any, Dict, Optional


class CreateStoredProcedure:
    def create_stored_procedure_from_file(self, sql_file_path: str, database_name: Optional[str] = None, 
                                        schema_name: Optional[str] = None, replace_if_exists: bool = True) -> Dict[str, Any]:
        """Create a stored procedure in Snowflake from a .sql file."""
        
        # Validate file exists and is readable
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        
        if not sql_file_path.lower().endswith('.sql'):
            raise ValueError(f"File must have .sql extension: {sql_file_path}")
        
        try:
            # Read the SQL file content
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read().strip()
                
            if not sql_content:
                raise ValueError(f"SQL file is empty: {sql_file_path}")
            
            # Try to extract procedure name from the SQL for better error reporting
            procedure_name = "UNKNOWN"
            procedure_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:([^.]+)\.)?(?:([^.]+)\.)?([^\s(]+)'
            match = re.search(procedure_pattern, sql_content, re.IGNORECASE | re.MULTILINE)
            if match:
                groups = match.groups()
                if groups[2]:  # procedure name
                    procedure_name = groups[2]
                elif groups[1]:  # schema.procedure
                    procedure_name = f"{groups[1]}.{groups[2]}" if groups[2] else groups[1]
                elif groups[0]:  # database.schema.procedure
                    parts = [g for g in groups if g]
                    procedure_name = ".".join(parts)
            
            # Set database and schema context if provided
            conn = self.verify_link()
            setup_commands = []
            
            if database_name:
                setup_commands.append(f"USE DATABASE {database_name}")
            if schema_name:
                setup_commands.append(f"USE SCHEMA {schema_name}")
            
            # Execute setup commands and the stored procedure creation
            start_time = time.time()
            results = []
            
            with conn.cursor() as cursor:
                # Set context
                for setup_cmd in setup_commands:
                    try:
                        cursor.execute(setup_cmd)
                        results.append({
                            "statement": setup_cmd,
                            "status": "success"
                        })
                    except Exception as e:
                        # Note: logger not available in modular structure, skip logging
                        pass
                
                # Execute the stored procedure creation
                try:
                    cursor.execute(sql_content)
                    execution_time = time.time() - start_time
                    
                    # Get procedure information after creation
                    proc_info_query = f"""
                    SELECT 
                        procedure_name,
                        procedure_schema,
                        procedure_catalog,
                        argument_signature,
                        data_type,
                        created,
                        last_altered,
                        procedure_definition
                    FROM information_schema.procedures 
                    WHERE procedure_name = '{procedure_name.split('.')[-1]}'
                    """
                    
                    if database_name:
                        proc_info_query += f" AND procedure_catalog = '{database_name}'"
                    if schema_name:
                        proc_info_query += f" AND procedure_schema = '{schema_name}'"
                    
                    proc_info_query += " ORDER BY created DESC LIMIT 1"
                    
                    try:
                        cursor.execute(proc_info_query)
                        proc_columns = [col[0] for col in cursor.description]
                        proc_rows = cursor.fetchall()
                        procedure_info = dict(zip(proc_columns, proc_rows[0])) if proc_rows else {}
                    except Exception as e:
                        # Note: logger not available, skip warning
                        procedure_info = {}
                    
                    return {
                        "success": True,
                        "procedure_name": procedure_name,
                        "sql_file_path": sql_file_path,
                        "execution_time": execution_time,
                        "database_context": database_name,
                        "schema_context": schema_name,
                        "procedure_info": procedure_info,
                        "setup_commands": results,
                        "message": f"Stored procedure '{procedure_name}' created successfully"
                    }
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    error_message = f"Failed to create stored procedure: {str(e)}"
                    
                    return {
                        "success": False,
                        "procedure_name": procedure_name,
                        "sql_file_path": sql_file_path,
                        "execution_time": execution_time,
                        "database_context": database_name,
                        "schema_context": schema_name,
                        "error": error_message,
                        "setup_commands": results,
                        "sql_content_preview": sql_content[:500] + "..." if len(sql_content) > 500 else sql_content
                    }
                    
        except Exception as e:
            error_message = f"Error processing SQL file: {str(e)}"
            return {
                "success": False,
                "sql_file_path": sql_file_path,
                "error": error_message,
                "database_context": database_name,
                "schema_context": schema_name
            }