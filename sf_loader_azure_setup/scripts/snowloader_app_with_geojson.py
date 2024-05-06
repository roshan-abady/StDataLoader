import re
import getpass
import warnings
import threading
import pandas as pd
import streamlit as st
# import geopandas as gpd
from snowflake.snowpark import Session

warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

st.title("Snowloader")
st.write(
    """Drag and Drop the ***CSV*** or ***Excel file***, you want to load into snowflake,
    or select it using the ***Browse files*** button.
        """
)

col1, col2 = st.columns(2)

# Hard-coded Snowflake configuration
config = {
    'Snowflake': {
        'User': f"{getpass.getuser()}@myob.com",
        'Password': st.secrets["Snowflake"]["Password"],
        'Account': st.secrets["Snowflake"]["Account"],
        'Authenticator': st.secrets["Snowflake"]["Authenticator"],
        'Role': st.secrets["Snowflake"]["Role"],
        'Warehouse': st.secrets["Snowflake"]["Warehouse"],
        'Database': st.secrets["Snowflake"]["Database"],
        'Schema': st.secrets["Snowflake"]["Schema"],
    }
}

def create_snowflake_session(config):
    """
    Create a Snowflake session using the provided configuration.

    Args:
        config (dict): A dictionary containing the Snowflake configuration.

    Returns:
        Session: A Snowflake session.
    """
    return Session.builder.configs(config).create()

def format_table_name(name):
    name = re.sub(r'\W+', '_', name)  # Replace non-alphanumeric characters with underscores
    return name.upper()

def snowflake_upload_operation(table_name, df, config, results):
    try:
        session = create_snowflake_session(config)
        print(session)
        tables = session.sql(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {config['Schema']}").collect()

        if tables:  # If table already exists
            results["exists"] = True
        else:  # If table does not exist
            session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
            results["success"] = True
    except Exception as e:
        results["error"] = str(e)

def calculate_midpoint(geometry):
    if geometry is not None:
        bbox = geometry.bounds  # bounds method returns a tuple (minx, miny, maxx, maxy)
        midpoint_x = (bbox[0] + bbox[2]) / 2
        midpoint_y = (bbox[1] + bbox[3]) / 2
        return pd.Series([midpoint_x, midpoint_y], index=['midpoint_x', 'midpoint_y'])
    else:
        return pd.Series([None, None], index=['midpoint_x', 'midpoint_y'])

def clean_string(s):
    if isinstance(s, str):
        s = s.translate(str.maketrans('', '', '\'\"[]{}()'))  # Removes ' " [ ] { } ( )
    return s

with col2:
    try:
        uploaded_file = st.file_uploader("Choose a CSV, Excel, or GeoJSON file", type=["csv", "xls", "xlsx"])  # Add new file types i.e. "geojson" to the list of accepted file types

        if uploaded_file:
            file_type = uploaded_file.name.split(".")[-1]

            if file_type not in ["csv", "xls", "xlsx", "geojson"]:  # Add "geojson" to the list of supported file types
                st.error("File type not supported.")

            default_table_name = format_table_name(uploaded_file.name.split(".")[0])
            table_name = st.text_input("Table Name:", default_table_name)

            if file_type == "csv":
                df = pd.read_csv(uploaded_file, low_memory=False) 
            elif file_type in ["xls", "xlsx"]:
                df = pd.read_excel(uploaded_file)  
            # # Extract the midpoints directly after reading the GeoJSON
            # elif file_type == "geojson":  # Add this new block to handle GeoJSON files
            #     df = gpd.read_file(uploaded_file).to_crs(epsg=4326)  # Read GeoJSON into a GeoDataFrame     

            #     # Calculate the midpoint and add as new columns
            #     df[['midpoint_x', 'midpoint_y']] = df['geometry'].apply(calculate_midpoint)     

            #     # Drop the geometry column as it's no longer needed
            #     df.drop('geometry', axis=1, inplace=True)       

            #     # Clean the DataFrame of quotation marks and brackets
            #     df['geo_point_2d'] = df['geo_point_2d'].astype(str)
            #     df['ste_code'] = df['ste_code'].astype(str)
            #     df['ste_name'] = df['ste_name'].astype(str)
            #     df['lga_code'] = df['lga_code'].astype(str)
            #     df['lga_name'] = df['lga_name'].astype(str)
            #     df['scc_code'] = df['scc_code'].astype(str)
            #     df['scc_name'] = df['scc_name'].astype(str)
                
            #     for col in df.select_dtypes(include=['object']).columns:
            #         df[col] = df[col].apply(clean_string)
            #     # Convert GeoDataFrame to a regular DataFrame (use Modin if you've replaced pandas with Modin)
            #     df = pd.DataFrame(df)       

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
