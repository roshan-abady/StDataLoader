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

def preview_df_data_types_streamlit(df):
    # Start building the markdown string for an HTML table
    markdown_str = "<table><tr><th>Column Name</th><th>Data Type</th></tr>"
    
    # Add a row for each column and its data type
    for column_name, data_type in df.dtypes.items():
        markdown_str += f"<tr><td>{column_name}</td><td>{data_type}</td></tr>"
    
    markdown_str += "</table>"
    
    # Display the markdown in Streamlit
    st.markdown(markdown_str)


def snowflake_upload_operation(session, table_name, df, table_schemas):
    try:
        # Preview DataFrame data types
        preview_df_data_types_streamlit(df)
        
        df = adjust_dataframe_types_using_schema(df, table_name, table_schemas)
        
        session.sql(f"DELETE FROM {table_name}").collect()
        st.success(f"Table {table_name} has been truncated.")
        session.write_pandas(df, table_name, auto_create_table=True)
        st.success(f"Successfully uploaded data to Snowflake table {table_name}.")
        return True, None
    except Exception as e:
        return False, str(e)
    
def fetch_and_preview_ddl(session, table_name):
    try:
        ddl_result = session.sql(f"SELECT GET_DDL('TABLE', '{table_name}')").collect()[0][0]
        st.text_area("Table DDL:", ddl_result, height=300)
        return ddl_result
    except Exception as e:
        st.error(f"Error fetching DDL for {table_name}: {e}")
        return ""

def init_or_update_snowflake_session(config):
    try:
        st.session_state.snowflake_session = Session.builder.configs(config).create()
        session_status = check_session_status(st.session_state.snowflake_session)
    except Exception as e:
        st.error(f"Failed to create/update Snowflake session: {e}")
        st.session_state.snowflake_session = None

def fetch_available_roles(session, database_name):
    try:
        result = session.sql("SELECT CURRENT_AVAILABLE_ROLES()").collect()
        if result:
            roles_list = json.loads(result[0][0])
            relevant_roles = sorted([role for role in roles_list if role.startswith(database_name)])
            return relevant_roles
    except Exception as e:
        st.error(f"Failed to fetch available roles: {e}")
        return []
    return []

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
    available_roles = fetch_available_roles(session, default_config["database"])
    st.title("Snowloader")
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"])
    if uploaded_file is not None:
        file_type = uploaded_file.name.split(".")[-1]
        if file_type in ["csv", "xls", "xlsx"]:
            default_table_name = format_table_name(uploaded_file.name.split(".")[0])
            table_name = st.text_input("Edit Table Name:", value=default_table_name if default_table_name else "table_name")
            table_name = format_table_name(table_name)
            st.write("**Table Name Preview:**")
            st.info(f"{table_name}")
            ddl = fetch_and_preview_ddl(session, table_name)
            if file_type == "csv":
                df = pd.read_csv(uploaded_file, low_memory=False)
            else:
                df = pd.read_excel(uploaded_file)
            # Preview data types here before further operations
            # df_data_types = preview_df_data_types_streamlit(df)
            st.write("Preview of Data:")
            st.dataframe(df.head())
            if st.button("Upload to Snowflake"):
                with st.spinner("Uploading to Snowflake..."):
                    success, error = snowflake_upload_operation(session, table_name, df, table_schemas)
                    if success:
                        st.success(f"Successfully Uploaded/Overwritten to Snowflake table {table_name}", icon="✅")
                    else:
                        st.error(f"An error occurred: {error}")