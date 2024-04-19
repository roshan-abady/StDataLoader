import re
import os
import json
import getpass
import warnings
import pandas as pd
import streamlit as st

from snowflake.snowpark import Session

warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

def check_session_status(session):
    try:
        result = session.sql("SELECT CURRENT_TIMESTAMP").collect()
        if result:
            return "Active"
    except Exception as e:
        return f"Failed to execute query. Error: {e}"
    return "Inactive"

def format_table_name(name):
    name = re.sub(r'\W+', '_', name)
    return name.upper()

def fetch_and_preview_ddl(session, table_name):
    try:
        ddl_result = session.sql(f"SELECT GET_DDL('TABLE', '{table_name}')").collect()[0][0]
        st.write("Table DDL:")
        st.code(ddl_result, language="sql", line_numbers=True)
        return ddl_result
    except Exception as e:
        st.error(f"Error fetching DDL for {table_name}: {e}")
        return ""

def parse_sql_schema_file(table_name):
    schema_dict = {}
    try:
        # Construct the file path
        file_path = os.path.join('./sql', f'{table_name}.sql')
        
        # Read the file content
        with open(file_path, 'r') as file:
            sql_content = file.read()
        
        # Regular expression to extract column definitions focusing on capturing data types within parentheses
        pattern = re.compile(r'(\w+)\s+(\w+)\(([\d,]+)\)')
        matches = pattern.findall(sql_content)
        
        for match in matches:
            column_name, data_type, details = match
            full_type = f"{data_type}({details})"  # Construct full data type description
            schema_dict[column_name] = full_type  # Map column names to full data type description
        
        return schema_dict
    
    except FileNotFoundError:
        st.error(f"Schema file for {table_name} not found.")
        return None
    except Exception as e:
        st.error(f"An error occurred while parsing the schema file: {e}")
        return None

def modify_snowflake_connection_parameters(default_config, col2):
    with col2.expander("Snowflake Connection Configuration"):
        editable_keys = ['user']  # Editable keys for user inputs
        radio_keys = ['role', 'schema']  # Schema and role selections via radio
        schema_options = ['TRANSFORMED_PROD', 'RAW']  # Schema options
        specific_roles = ['OPERATIONS_ANALYTICS_OWNER', 'OPERATIONS_ANALYTICS_OWNER_AD']  # Role options

        for key in editable_keys:
            default_config[key] = col2.text_input(f"{key}", value=default_config[key])

        if 'schema' in radio_keys:
            selected_schema = col2.radio("Schema", options=schema_options, index=schema_options.index(default_config['schema']) if default_config['schema'] in schema_options else 0)
            default_config['schema'] = selected_schema

        if 'role' in radio_keys:
            available_roles = [role for role in specific_roles if role in specific_roles]
            if available_roles:
                selected_role = col2.radio("Role", options=available_roles, index=0 if available_roles else None)
                default_config['role'] = selected_role

        if col2.button("Update Connection"):
            init_or_update_snowflake_session(default_config)

def init_or_update_snowflake_session(config):
    try:
        st.session_state.snowflake_session = Session.builder.configs(config).create()
        session_status = check_session_status(st.session_state.snowflake_session)
        if session_status == "Active":
            st.success("Snowflake session is active.")
        else:
            st.error("Snowflake session is inactive.")
    except Exception as e:
        st.error(f"Failed to create/update Snowflake session: {e}")
        st.session_state.snowflake_session = None

def adjust_dataframe_types_using_schema(df, schema):
    # Iterate through each column and change its data type according to the schema dictionary
    for column, dtype in schema.items():
        if column in df.columns:
            try:
                # Convert column type based on the schema
                if 'NUMBER' in dtype:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif 'VARCHAR' in dtype or 'TEXT' in dtype:
                    df[column] = df[column].astype(str)
                elif 'BOOLEAN' in dtype:
                    df[column] = df[column].astype(bool)
                elif 'DATE' in dtype or 'TIMESTAMP' in dtype:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
            except Exception as e:
                st.error(f"Could not convert column {column} to {dtype}: {e}")
    return df


def snowflake_upload_operation(session, table_name, df):
    try:
        df = adjust_dataframe_types_using_schema(df, parse_sql_schema_file(table_name))
        session.sql(f"DELETE FROM {table_name}").collect()
        st.success(f"Table {table_name} has been truncated.")
        session.write_pandas(df, table_name, auto_create_table=True)
        st.success(f"Successfully uploaded data to Snowflake table {table_name}.")
        return True, None
    except Exception as e:
        return False, str(e)

# Default configuration for Snowflake connection
default_config = {
    "user": f"{getpass.getuser()}@myob.com",
    "password": '',
    "account": 'bu20658.ap-southeast-2',
    "authenticator": 'externalbrowser',
    "role": 'OPERATIONS_ANALYTICS_OWNER_AD',
    "warehouse": 'OPERATIONS_ANALYTICS_WAREHOUSE_PROD',
    "database": 'OPERATIONS_ANALYTICS',
    "schema": 'TRANSFORMED_PROD',
    "client_session_keep_alive": True,
}

# Main Streamlit UI layout
col1, col2 = st.columns(2)
with col1:
    st.title("Data Management")
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"], key="file_uploader")
    if uploaded_file:
        file_type = uploaded_file.name.split(".")[-1]
        table_name = format_table_name(uploaded_file.name.split(".")[0])
        table_schema = parse_sql_schema_file(table_name)

        if table_schema:
            st.write("Data Definition:")
            st.json(table_schema)
            df = pd.read_csv(uploaded_file) if file_type == 'csv' else pd.read_excel(uploaded_file)
            st.write("Preview of Data:")
            st.dataframe(df.head())
            if st.button("Upload to Snowflake", key="upload_button"):
                with st.spinner("Uploading to Snowflake..."):
                    success, error = snowflake_upload_operation(st.session_state.snowflake_session, table_name, df)
                    if success:
                        st.success(f"Successfully Uploaded/Overwritten to Snowflake table {table_name}")
                    else:
                        st.error(f"An error occurred: {error}")

with col2:
    if 'snowflake_session' not in st.session_state:
        init_or_update_snowflake_session(default_config)

    session = st.session_state.get('snowflake_session')
    if session is None:
        st.error("Failed to initialize Snowflake session.")
    else:
        modify_snowflake_connection_parameters(default_config, col2)
