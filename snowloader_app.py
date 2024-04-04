import re
import json
import getpass
import warnings
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

# Add a function to change the table ownership
def change_table_ownership(session, table_name, new_owner_role):
    """
    Changes the ownership of the specified table to the given role.
    
    Args:
    - session: The active Snowflake session.
    - table_name: Name of the table for which to change ownership.
    - new_owner_role: The role to which ownership of the table will be transferred.
    """
    try:
        # Use the SQL to change the ownership of the table
        session.sql(f"GRANT OWNERSHIP ON TABLE {table_name} TO ROLE {new_owner_role}").collect()
        st.success(f"Ownership of table {table_name} changed to {new_owner_role}.")
    except Exception as e:
        st.error(f"Failed to change ownership of table {table_name}: {e}")
        
# Define the function to check the Snowflake session status
def check_session_status(session):
    try:
        # Attempt a simple query to check the session
        result = session.sql("SELECT CURRENT_TIMESTAMP").collect()
        if result:
            # If the query succeeds, the session is active
            return "Active"
    except Exception as e:
        # If an exception occurs, the session is not active
        return f"Failed to execute query. Error: {e}"
    return "Inactive"

# Function to format table name
def format_table_name(name):
    name = re.sub(r'\W+', '_', name)  # Replace non-alphanumeric characters with underscores
    return name.upper()

# Function for upload operation with automatic overwrite
def snowflake_upload_operation(table_name, df):
    try:
        # Overwrite table if it exists, create a new one if it doesn't
        session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
        return True, None
    except Exception as e:
        return False, str(e)

# Function to initialize or update the Snowflake session
def init_or_update_snowflake_session(config):
    try:
        st.session_state.snowflake_session = Session.builder.configs(config).create()
        session_status = check_session_status(st.session_state.snowflake_session)
    except Exception as e:
        st.error(f"Failed to create/update Snowflake session: {e}")
        st.session_state.snowflake_session = None

# Function to fetch available roles and filter them based on the database name
def fetch_available_roles(session, database_name):
    try:
        # Execute the query to get available roles
        result = session.sql("SELECT CURRENT_AVAILABLE_ROLES()").collect()
        if result:
            # Assuming the result is a string representation of a list
            roles_list = json.loads(result[0][0])
            # Filter roles that start with the database name
            relevant_roles = sorted([role for role in roles_list if role.startswith(database_name)])
            return relevant_roles
    except Exception as e:
        st.error(f"Failed to fetch available roles: {e}")
        return []
    return []

# Default Snowflake connection parameters with added heartbeat frequency
default_config = {
    "user": f"{getpass.getuser()}@myob.com",
    "password": '',
    "account": 'bu20658.ap-southeast-2',
    "authenticator": 'externalbrowser',
    "role": 'OPERATIONS_ANALYTICS_MEMBER_AD',
    "warehouse": 'OPERATIONS_ANALYTICS_WAREHOUSE_PROD',
    "database": 'OPERATIONS_ANALYTICS',
    "schema": 'RAW',
    "client_session_keep_alive": True,
}

# Initialize or update Snowflake session
if 'snowflake_session' not in st.session_state:
    init_or_update_snowflake_session(default_config)
session = st.session_state.get('snowflake_session')

if session is None:
    st.error("Failed to initialize Snowflake session.")
else:
    available_roles = fetch_available_roles(session, default_config["database"])
    current_schema = session.sql(f"SELECT CURRENT_SCHEMA()").collect()[0]

    st.title("Snowloader")
    st.write("""Drag and Drop the ***CSV*** or ***Excel file*** you want to load into Snowflake,
              or select it using the ***Browse files*** button.""")

    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"])
    if uploaded_file is not None:
        file_type = uploaded_file.name.split(".")[-1]
        if file_type in ["csv", "xls", "xlsx"]:
            default_table_name = format_table_name(uploaded_file.name.split(".")[0])
            table_name = st.text_input("Edit Table Name:", value=default_table_name if default_table_name else "table_name")
            table_name = format_table_name(table_name)
            st.write(f"**Table Name Preview:**")
            st.info(f"{table_name}")
            if file_type == "csv":
                df = pd.read_csv(uploaded_file, low_memory=False)
            else:
                df = pd.read_excel(uploaded_file)

            st.write("Preview of Data:")
            st.dataframe(df.head())
        with st.spinner("Uploading to Snowflake..."):
            if st.button("Upload to Snowflake"):
                success, error = snowflake_upload_operation(table_name, df)
                if success:
                    st.success(f"Successfully Uploaded/Overwritten to Snowflake table {table_name}", icon="✅")
                    
                    # Example condition to change table ownership
                    if table_name == "your_specific_table_name":
                        change_table_ownership(session, table_name, "OPERATIONS_ANALYTICS_TRANSFORM_RUNNER_PROD")
                else:
                    st.error(f"An error occurred: {error}")

    # UI for modifying Snowflake connection parameters at the bottom
    with st.expander("Snowflake Connection Configuration"):
        editable_keys = ['schema', 'user']
        for key in default_config.keys():
            if key in editable_keys:
                default_config[key] = st.text_input(f"{key}", value=default_config[key])
            elif key == "role":
                # Dropdown for 'role' parameter using dynamically fetched roles
                default_config["role"] = st.selectbox("role", options=available_roles, index=available_roles.index(default_config["role"]))
            else:
                st.text(f"{key}")
                st.info(default_config[key])

        if st.button("Update Connection"):
            init_or_update_snowflake_session(default_config)
