import os
import re
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

def create_stage(session, stage_name):
    session.sql(f"CREATE OR REPLACE STAGE {stage_name}").collect()
    st.info(f"Stage {stage_name} created or replaced.")

def upload_data_to_stage(session, stage_name, file_path):
    session.file.put(file_path, stage_name)
    st.success(f"Data uploaded to stage {stage_name}.")

def truncate_table(session, table_name):
    try:
        session.sql(f"TRUNCATE TABLE IF EXISTS {table_name}").collect()
        st.success(f"Table {table_name} successfully truncated.")
    except Exception as e:
        st.error(f"Failed to truncate table {table_name}: {e}")

def bulk_copy_into(session, stage_name, table_name):
    try:
        session.sql(f"COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT = (TYPE={file_type},SKIP_HEADER=1,FIELD_DELIMITER=',',TRIM_SPACE=FALSE,FIELD_OPTIONALLY_ENCLOSED_BY=NONE,REPLACE_INVALID_CHARACTERS=TRUE,DATE_FORMAT='YYYY-MM-DD',TIME_FORMAT='HH24:MI:SS.FF',TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS TZH:TZM') ON_ERROR=CONTINUE").collect()
        st.success("Data successfully copied with schema auto-detection.")
    except Exception as e:
        st.error(f"Failed to copy data with schema auto-detection: {e}")

def snowflake_upload_with_stage(session, df, stage_name, table_name):
    temp_file = f"./temp/temp_{table_name}.csv"
    df.to_csv(temp_file, index=False)
    create_stage(session, stage_name)
    upload_data_to_stage(session, stage_name, temp_file)
    truncate_table(session, table_name)
    bulk_copy_into(session, stage_name, table_name)
    os.remove(temp_file)

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
        
def modify_snowflake_connection_parameters(default_config):
        st.write("Snowflake Connection Configuration:")
        st.info(default_config['user'])
        radio_keys = ['role', 'schema']
        schema_options = ['TRANSFORMED_PROD', 'RAW']
        available_roles = ['OPERATIONS_ANALYTICS_OWNER', 'OPERATIONS_ANALYTICS_OWNER_AD']
        
        if 'schema' in radio_keys:
            selected_schema = st.radio("Schema", options=schema_options, index=schema_options.index(default_config['schema']) if default_config['schema'] in schema_options else 0)
            default_config['schema'] = selected_schema

            selected_role = st.radio("Role", options=available_roles, index=0 if available_roles else None)
            default_config['role'] = selected_role
            init_or_update_snowflake_session(default_config)

                
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
# Streamlit UI layout
st.title("Data Management")
uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"], key="file_uploader")
modify_snowflake_connection_parameters(default_config)
if uploaded_file:
    file_type = uploaded_file.name.split(".")[-1]
    table_name = format_table_name(uploaded_file.name.split(".")[0])
    df = pd.read_csv(uploaded_file) if file_type == 'csv' else pd.read_excel(uploaded_file)
    st.write("Preview of Data:")
    st.dataframe(df.head())
    
    if st.button("Upload to Snowflake", key="upload_button"):
        with st.spinner("Uploading to Snowflake..."):
            stage_name = f"{table_name}_stage"
            if 'snowflake_session' not in st.session_state:
                init_or_update_snowflake_session(default_config)

            session = st.session_state.get('snowflake_session')
            if session is None:
                st.error("Failed to initialize Snowflake session.")
            else:
                modify_snowflake_connection_parameters(default_config)
            snowflake_upload_with_stage(session, df, stage_name, table_name)