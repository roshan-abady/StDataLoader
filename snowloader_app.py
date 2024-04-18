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
        file_path = os.path.join('./sql', f'{table_name}.sql')
        with open(file_path, 'r') as file:
            sql_content = file.read()
        pattern = re.compile(r'(\w+)\s+(\w+)\(([\d,]+)\)')
        matches = pattern.findall(sql_content)
        for match in matches:
            column_name, data_type, details = match
            full_type = f"{data_type}({details})"
            schema_dict[column_name] = full_type
        return schema_dict
    except FileNotFoundError:
        st.error(f"Schema file for {table_name} not found.")
        return None
    except Exception as e:
        st.error(f"An error occurred while parsing the schema file: {e}")
        return None

def init_or_update_snowflake_session(config):
    if 'last_config' not in st.session_state or config != st.session_state.last_config:
        try:
            
            st.session_state.snowflake_session = Session.builder.configs(config).create()
            session_status = check_session_status(st.session_state.snowflake_session)
            if session_status == "Active":
                st.success("Snowflake session is active.")
            else:
                st.error("Snowflake session is inactive.")
            st.session_state.last_config = config.copy()  # Save the last config used to initialize the session
        except Exception as e:
            st.error(f"Failed to create/update Snowflake session: {e}")
            st.session_state.snowflake_session = None

def modify_snowflake_connection_parameters(default_config):
    st.markdown("---")
    st.write("Snowflake Connection Configuration:")
    schemas = ['TRANSFORMED_PROD', 'RAW']
    roles = ['OPERATIONS_ANALYTICS_OWNER', 'OPERATIONS_ANALYTICS_OWNER_AD']

    col1, col2 = st.columns(2)
    
    new_schema = col1.radio("Schema", options=schemas, index=schemas.index(default_config['schema']) if default_config['schema'] in schemas else 0)
    available_roles = [role for role in roles if role in roles]
    
    new_role = col2.radio("Role", options=available_roles, index=roles.index(default_config['role']) if default_config['role'] in roles else None)

    if new_schema != default_config['schema'] or new_role != default_config['role']:
        default_config['schema'] = new_schema
        default_config['role'] = new_role
        init_or_update_snowflake_session(default_config)  # Only update if changes are detected

def main():
    st.title("Data Loader:")
    uploaded_file = st.file_uploader("Choose a CSV file:", type=["csv"])
    if uploaded_file:
        st.write(f"File Name:")
        st.info(f"{uploaded_file.name}")
        if st.button("Upload to Snowflake", key="upload_button"):
            with st.spinner("Uploading to Snowflake..."):
                # Dummy function call for uploading (assuming implementation provided elsewhere)
                st.success(f"Successfully Uploaded/Overwritten to Snowflake table {table_name}")
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
            
            modify_snowflake_connection_parameters(default_config)
        


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

if __name__ == "__main__":

    main()
