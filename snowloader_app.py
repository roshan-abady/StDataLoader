#!/usr/bin/env python
# snowloader.py
import re
import toml
import getpass
import warnings
import threading
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
# for headless browser automation
import io
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

warnings.filterwarnings(
    "ignore", category=FutureWarning, module="pyarrow.pandas_compat"
)

st.title("Snowloader")
st.write(
    """Drag and Drop the ***CSV*** file you want to load into snowflake,
    or select it using the ***Browse files*** button.
        """
)
secrets = toml.load(".streamlit/credentials.toml")
config = {
    "Snowflake": {
        "User": f"{getpass.getuser()}@myob.com",
        "Password": secrets["Snowflake"]["Password"],
        "Account": secrets["Snowflake"]["Account"],
        "Authenticator": secrets["Snowflake"]["Authenticator"],
        "Role": secrets["Snowflake"]["Role"],
        "Warehouse": secrets["Snowflake"]["Warehouse"],
        "Database": secrets["Snowflake"]["Database"],
        "Schema": secrets["Snowflake"]["Schema"],
    }
}
col1, col2 = st.columns(2)

# Allow user to edit config values
with st.expander("Edit Snowflake Configuration"):
    config["Snowflake"]["User"] = st.text_input(
        "User", value=config["Snowflake"]["User"] or "default_user"
    )
    config["Snowflake"]["Password"] = st.text_input(
        "Password", value=config["Snowflake"]["Password"] or "", type="password"
    )
    config["Snowflake"]["Account"] = st.text_input(
        "Account", value=config["Snowflake"]["Account"] or "default_account"
    )
    config["Snowflake"]["Authenticator"] = st.text_input(
        "Authenticator", value=config["Snowflake"]["Authenticator"] or "default_authenticator"
    )
    config["Snowflake"]["Role"] = st.text_input(
        "Role", value=config["Snowflake"]["Role"] or "default_role"
    )
    config["Snowflake"]["Warehouse"] = st.text_input(
        "Warehouse", value=config["Snowflake"]["Warehouse"] or "default_warehouse"
    )
    config["Snowflake"]["Database"] = st.text_input(
        "Database", value=config["Snowflake"]["Database"] or "default_database"
    )
    config["Snowflake"]["Schema"] = st.text_input(
        "Schema", value=config["Snowflake"]["Schema"] or "default_schema"
    )
    

def format_table_name(name):
    name = re.sub(
        r"\W+", "_", name
    )  # Replace non-alphanumeric characters with underscores
    return name.upper()


def snowflake_upload_operation(table_name, df, config, results):
    try:
        # Redirect stdout to a string buffer
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        # Create the Snowflake session
        # This will print the URL to the string buffer instead of the terminal
        session = Session.builder.configs(config).create()

        # Reset stdout back to its original state
        sys.stdout = old_stdout

        # Extract the URL from the buffer
        # This assumes the URL is the last word in the output
        url = buffer.getvalue().split()[-1]

        # Set up Selenium to run in headless mode
        options = Options()
        options.headless = True
        
        # This option is often required in Docker/container environments
        options.add_argument("--no-sandbox") 
        # Overcomes limited resource problems
        options.add_argument("--disable-dev-shm-usage")  
        
        # Set up the Chrome service
        chrome_service = Service(ChromeDriverManager().install())
        # driver = webdriver.Chrome(options=options)

        # Open the URL in the browser
        driver = webdriver.Chrome(service=chrome_service, options=options)
        driver.get(url)

        # Don't forget to quit the driver when you're done
        driver.quit()
        
        tables = session.sql(
            f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {config['Schema']}"
        ).collect()

        if tables:  # If table already exists
            results["exists"] = True
        else:  # If table does not exist
            session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
            results["success"] = True
    except Exception as e:
        results["error"] = str(e)


def clean_string(s):
    if isinstance(s, str):
        s = s.translate(str.maketrans("", "", "'\"[]{}()"))  # Removes ' " [ ] { } ( )
    return s


with col2:
    try:
        uploaded_file = st.file_uploader(
            "CSV File Only", type=["csv"]
        )  # Add a new file types i.e. "geojson", "xls", "xlsx" to the list of accepted file types

        if uploaded_file:
            file_type = uploaded_file.name.split(".")[-1]

            if file_type not in [
                "csv",
                "xls",
                "xlsx",
            ]:  # Add a new file types i.e. "geojson" to the list of supported file types
                st.error("File type not supported.")

            default_table_name = format_table_name(uploaded_file.name.split(".")[0])
            table_name = st.text_input("Table Name:", default_table_name)

            if file_type == "csv":
                df = pd.read_csv(uploaded_file, low_memory=False, encoding="ISO-8859-1")
            elif file_type in ["xls", "xlsx"]:
                df = pd.read_excel(uploaded_file, encoding="ISO-8859-1")
            # Moved preview to col1
            with col1:
                st.write("Preview of Data:")
                st.write(df.head())
            results = {"exists": False, "success": False, "error": None}
            if st.button("Upload to Snowflake"):
                formatted_table_name = format_table_name(table_name)
                thread = threading.Thread(
                    target=snowflake_upload_operation,
                    args=(formatted_table_name, df, config["Snowflake"], results),
                )
                thread.start()
                thread.join()

                if results["error"]:
                    st.error(f"An error occurred: {results['error']}")
                elif results["exists"]:
                    if "confirmed_overwrite" not in st.session_state:
                        if st.button(
                            "Table Name already exists! Change the Table Name. Or if you'd like to overwrite the existing table click this button."
                        ):
                            st.session_state.confirmed_overwrite = True
                            st.success(
                                f"Uploaded data to Snowflake table {formatted_table_name}."
                            )
            if results["success"]:
                st.success(f"Uploaded data to Snowflake table {formatted_table_name}.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
