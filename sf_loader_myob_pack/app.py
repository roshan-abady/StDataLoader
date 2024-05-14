import os
import re
import getpass
import warnings
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

def load_mapping():
    # Load the mapping CSV from the 'mapping' folder
    return pd.read_csv('sf_loader_myob_pack/mapping/mapping.csv')

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
    st.info(f":building_construction: Stage created.")

def upload_data_to_stage(session, stage_name, file_path):
    session.file.put(file_path, stage_name)
    st.info(f":clapper: Data Staged.")

def truncate_table(session, table_name):
    try:
        session.sql(f"TRUNCATE TABLE IF EXISTS {table_name}").collect()
        st.info(f":haircut: Table Truncated.")
    except Exception as e:
        st.error(f"Failed to truncate table {table_name}: {e}")

def bulk_copy_into(session, stage_name, table_name, file_type):
    try:
        session.sql(f"COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT = (TYPE='{file_type}', SKIP_HEADER=1, FIELD_DELIMITER=',', TRIM_SPACE=FALSE, FIELD_OPTIONALLY_ENCLOSED_BY='\"', REPLACE_INVALID_CHARACTERS=TRUE, DATE_FORMAT='YYYY-MM-DD', TIME_FORMAT='HH24:MI:SS.FF', TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS TZH:TZM') ON_ERROR=CONTINUE").collect()
        st.success(":white_check_mark: Data Loaded.")
    except Exception as e:
        st.error(f"Failed to copy data: {e}")

def snowflake_upload_with_stage(session, df, stage_name, table_name, file_type):
    temp_file = f"{table_name}.csv"
    df.to_csv(temp_file, index=False)
    create_stage(session, stage_name)
    upload_data_to_stage(session, stage_name, temp_file)
    truncate_table(session, table_name)
    bulk_copy_into(session, stage_name, table_name, file_type)
    os.remove(temp_file)

def modify_snowflake_connection_parameters(default_config):
    schema_options = ['TRANSFORMED_PROD', 'RAW']
    available_roles = ['OPERATIONS_ANALYTICS_OWNER_AD', 'OPERATIONS_ANALYTICS_OWNER']
    st.write(f"Hi {name} \n\n Here you can pick a different **Schema** and **Role**")
    col1, col2 = st.columns(2)
    selected_schema = col1.radio("", options=schema_options, index=schema_options.index(default_config['schema']))
    selected_role = col2.radio(" ", options=available_roles, index=available_roles.index(default_config['role']))
    if selected_schema != default_config['schema'] or selected_role != default_config['role']:
        default_config['schema'] = selected_schema
        default_config['role'] = selected_role
        st.session_state.snowflake_session = Session.builder.configs(default_config).create()
        
username=getpass.getuser()
name=username.split('.')[0]
# Default configuration for Snowflake connection
default_config = {
    "user": f"{username}@myob.com",
    "password": '',
    "account": 'bu20658.ap-southeast-2',
    "authenticator": 'externalbrowser',
    "role": 'OPERATIONS_ANALYTICS_OWNER_AD',
    "warehouse": 'OPERATIONS_ANALYTICS_WAREHOUSE_PROD',
    "database": 'OPERATIONS_ANALYTICS',
    "schema": 'TRANSFORMED_PROD',
    "client_session_keep_alive": True,
}

# Initialize Snowflake session once when the app runs
if 'snowflake_session' not in st.session_state:
    st.session_state.snowflake_session = Session.builder.configs(default_config).create()

session = st.session_state.snowflake_session
if check_session_status(session) != "Active":
    st.error("Failed to initialise or session is inactive.")

mapping_df = load_mapping()  # Load the mapping data

# Streamlit UI layout
st.title(":snowflake: MYOB Loader")
modify_snowflake_connection_parameters(default_config)  # Display configuration options

uploaded_file = st.file_uploader("You can **Drop** your file down below or use the **Browse** button to pick a file", type=["csv"], key="file_uploader")

if uploaded_file:
    file_type = uploaded_file.name.split(".")[-1]
    table_name = format_table_name(uploaded_file.name.split(".")[0])
    df = pd.read_csv(uploaded_file) if file_type == 'csv' else pd.read_excel(uploaded_file)
    
    st.write("Preview of Data:")
    st.dataframe(df,height=200, use_container_width=True,) 

    # Check if there is a mapping entry for the uploaded file
    mapping = mapping_df[mapping_df['TABLE_NAME'].str.upper() == table_name.upper()]
    if not mapping.empty:
        # Update config if there are changes
        new_schema = mapping['TABLE_SCHEMA'].values[0]
        new_role = mapping['TABLE_OWNER'].values[0]
        if new_schema != default_config['schema'] or new_role != default_config['role']:
            default_config['schema'] = new_schema
            default_config['role'] = new_role
            session.reconfigure(default_config)  # Apply new configuration
            
    if st.button("Upload to Snowflake", key="upload_button"):
        with st.status("Uploading to Snowflake...",expanded=True) as status:
            stage_name = f"{table_name}_STAGE"
            if check_session_status(session) == "Active":
                snowflake_upload_with_stage(session, df, stage_name, table_name, file_type)
                status.update(label="Upload complete!",state="complete", expanded=False)
            else:
                st.error("Snowflake session is inactive or not initialized.")
