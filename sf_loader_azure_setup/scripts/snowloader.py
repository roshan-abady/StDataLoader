#!/usr/bin/env python
# snowloader.py
import re
import getpass
import warnings
import threading
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

st.title("Snowloader")
st.write(
    """Drag and Drop the ***CSV*** or ***Excel file***, you want to load into snowflake,
    or select it using the ***Browse files*** button.
        """
)

config = {
    "Snowflake": {
        "User": f"{getpass.getuser()}@myob.com",
        "Password": '',
        "Account": 'bu20658.ap-southeast-2',
        "Authenticator": 'externalbrowser',
        "Role": 'OPERATIONS_ANALYTICS_MEMBER_AD',
        "Warehouse":'OPERATIONS_ANALYTICS_WAREHOUSE_PROD',
        "Database": 'OPERATIONS_ANALYTICS',
        "Schema": 'RAW'
    }
}
col1, col2 = st.columns(2)

# Allow user to edit config values
with st.expander("Edit Snowflake Configuration"):
    config['Snowflake']['User'] = st.text_input("User", value=config['Snowflake']['User'])
    config['Snowflake']['Password'] = st.text_input("Password", value=config['Snowflake']['Password'], type="password")
    config['Snowflake']['Account'] = st.text_input("Account", value=config['Snowflake']['Account'])
    config['Snowflake']['Authenticator'] = st.text_input("Authenticator", value=config['Snowflake']['Authenticator'])
    config['Snowflake']['Role'] = st.text_input("Role", value=config['Snowflake']['Role'])
    config['Snowflake']['Warehouse'] = st.text_input("Warehouse", value=config['Snowflake']['Warehouse'])
    config['Snowflake']['Database'] = st.text_input("Database", value=config['Snowflake']['Database'])
    config['Snowflake']['Schema'] = st.text_input("Schema", value=config['Snowflake']['Schema'])


def format_table_name(name):
    name = re.sub(r'\W+', '_', name)  # Replace non-alphanumeric characters with underscores
    return name.upper()

def snowflake_upload_operation(table_name, df, config, results):
    try:
        session = Session.builder.configs(config).create()
        tables = session.sql(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {config['Schema']}").collect()

        if tables:  # If table already exists
            results["exists"] = True
        else:  # If table does not exist
            session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
            results["success"] = True
    except Exception as e:
        results["error"] = str(e)

def clean_string(s):
    if isinstance(s, str):
        s = s.translate(str.maketrans('', '', '\'\"[]{}()'))  # Removes ' " [ ] { } ( )
    return s

with col2:
    try:
        uploaded_file = st.file_uploader("CSV or Excel", type=["csv", "xls", "xlsx"])  # Add a new file types i.e. "geojson" to the list of accepted file types

        if uploaded_file:
            file_type = uploaded_file.name.split(".")[-1]

            if file_type not in ["csv", "xls", "xlsx"]:  # Add a new file types i.e. "geojson" to the list of supported file types
                st.error("File type not supported.")

            default_table_name = format_table_name(uploaded_file.name.split(".")[0])
            table_name = st.text_input("Table Name:", default_table_name)

            if file_type == "csv":
                df = pd.read_csv(uploaded_file, low_memory=False, encoding='ISO-8859-1') 
            elif file_type in ["xls", "xlsx"]:
                df = pd.read_excel(uploaded_file)
            # Moved preview to col1
            with col1:
                st.write("Preview of Data:")
                st.write(df.head())
            results = {"exists": False, "success": False, "error": None}
            if st.button("Upload to Snowflake"):
                formatted_table_name = format_table_name(table_name)
                thread = threading.Thread(target=snowflake_upload_operation, args=(formatted_table_name, df, config['Snowflake'], results))
                thread.start()
                thread.join()

                if results["error"]:
                    st.error(f"An error occurred: {results['error']}")
                elif results["exists"]:
                    if "confirmed_overwrite" not in st.session_state:
                        if st.button("Table Name already exists! Change the Table Name. Or if you'd like to overwrite the existing table click this button."):
                            st.session_state.confirmed_overwrite = True
                            st.success(f"Uploaded data to Snowflake table {formatted_table_name}.")
            if results["success"]:
                st.success(f"Uploaded data to Snowflake table {formatted_table_name}.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
