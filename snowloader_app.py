import re
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

def fetch_and_extract_schema(session, table_name):
    try:
        ddl_result = session.sql(f"SELECT GET_DDL('TABLE', '{table_name}')").collect()
        if ddl_result:
            ddl_str = ddl_result[0][0]
            return parse_ddl_to_schema(ddl_str)
        else:
            st.error(f"No DDL found for {table_name}")
            return {}
    except Exception as e:
        st.error(f"Error fetching DDL for {table_name}: {e}")
        return {}

def parse_ddl_to_schema(ddl_str):
    schema_dict = {}
    pattern = re.compile(r'\"(\w+)\"\s+(\w+)(?:\s+\w+)*,?')
    matches = pattern.findall(ddl_str)
    for match in matches:
        column_name, data_type = match[0], match[1]
        schema_dict[column_name] = map_snowflake_to_pandas_dtype(data_type)
    return schema_dict

def map_snowflake_to_pandas_dtype(snowflake_type):
    return {
        'VARCHAR': 'object', 'TEXT': 'object',
        'NUMBER': 'float64', 'INTEGER': 'int64',
        'FLOAT': 'float64', 'BOOLEAN': 'bool',
        'DATE': 'datetime64', 'TIMESTAMP': 'datetime64[ns]'
    }.get(snowflake_type, 'object')

def adjust_dataframe_types_using_schema(df, schema):
    for column, dtype in schema.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except Exception as e:
                st.warning(f"Could not convert column {column} to {dtype}: {e}")
    return df

def snowflake_upload_operation(session, table_name, df):
    try:
        df = adjust_dataframe_types_using_schema(df, fetch_and_extract_schema(session, table_name))
        session.sql(f"DELETE FROM {table_name}").collect()
        st.success(f"Table {table_name} has been truncated.")
        session.write_pandas(df, table_name, auto_create_table=True)
        st.success(f"Successfully uploaded data to Snowflake table {table_name}.")
        return True, None
    except Exception as e:
        return False, str(e)

def init_or_update_snowflake_session(config):
    try:
        st.session_state.snowflake_session = Session.builder.configs(config).create()
        session_status = check_session_status(st.session_state.snowflake_session)
    except Exception as e:
        st.error(f"Failed to create/update Snowflake session: {e}")
        st.session_state.snowflake_session = None

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

if 'snowflake_session' not in st.session_state:
    init_or_update_snowflake_session(default_config)

session = st.session_state.get('snowflake_session')

if session is None:
    st.error("Failed to initialize Snowflake session.")
else:
    st.title("Snowloader")
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"])
    if uploaded_file is not None:
        file_type = uploaded_file.name.split(".")[-1]
        default_table_name = format_table_name(uploaded_file.name.split(".")[0])
        table_name = format_table_name(default_table_name)
        st.write("**Table Name Preview:**")
        st.info(f"{table_name}")
        
        # Display DDL of the table
        if st.button("Show Table DDL"):
            fetch_and_extract_schema(session, table_name)

        if file_type == "csv":
            df = pd.read_csv(uploaded_file, low_memory=False)
        else:
            df = pd.read_excel(uploaded_file)
        st.write("Preview of Data:")
        st.dataframe(df.head())
        
        if st.button("Upload to Snowflake"):
            with st.spinner("Uploading to Snowflake..."):
                success, error = snowflake_upload_operation(session, table_name, df)
                if success:
                    st.success(f"Successfully Uploaded/Overwritten to Snowflake table {table_name}", icon="✅")
                else:
                    st.error(f"An error occurred: {error}")


# UI for modifying Snowflake connection parameters at the bottom
with st.expander("Snowflake Connection Configuration"):
    # Define keys that users can edit
    editable_keys = ['user']  # 'schema' is removed from here as we are providing a dropdown for it
    
    # Define keys that users can select from a dropdown
    dropdown_keys = ['role', 'schema']  # 'schema' added here for dropdown selection
    schema_options = ['TRANSFORMED_PROD', 'RAW']  # Options for schema dropdown
    
    # Display editable keys with text inputs
    for key in editable_keys:
        default_config[key] = st.text_input(f"{key}", value=default_config[key])

    # Dropdown for schema selection
    if 'schema' in dropdown_keys:
        selected_schema = st.selectbox("Schema", options=schema_options, index=schema_options.index(default_config['schema']) if default_config['schema'] in schema_options else 0)
        default_config['schema'] = selected_schema

    # Specific roles for dropdown (assuming you fetch roles somewhere)
    specific_roles = ['OPERATIONS_ANALYTICS_OWNER', 'OPERATIONS_ANALYTICS_MEMBER']  # Specific roles for dropdown
    available_roles = [role for role in specific_roles if role in specific_roles]  # Filter roles

    # Dropdown for selecting roles
    if 'role' in dropdown_keys:
        selected_role = st.selectbox("Role", options=available_roles, index=0 if available_roles else None)
        if available_roles:
            default_config['role'] = selected_role

    # Button to update connection
    if st.button("Update Connection"):
        init_or_update_snowflake_session(default_config)
