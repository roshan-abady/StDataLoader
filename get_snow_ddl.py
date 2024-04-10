import os
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Establish connection
con = snowflake.connector.connect(
    password = '',
    user = 'roshan.abady@myob.com',
    account = 'bu20658.ap-southeast-2',
    authenticator = 'externalbrowser',
    role = 'OPERATIONS_ANALYTICS_MEMBER_AD',
    warehouse = 'OPERATIONS_ANALYTICS_WAREHOUSE_PROD',
    database = 'OPERATIONS_ANALYTICS',
    schema = 'TRANSFORMED_PROD'
)

# Define the schemas you want to process
schemas_to_process = ['TRANSFORMED_PROD']

# Ensure the sql directory exists
os.makedirs('sql', exist_ok=True)

# For each schema, get all tables and views and their DDL
for schema_name in schemas_to_process:
    print(f"Processing schema: {schema_name}")
    con.cursor().execute(f"USE SCHEMA {schema_name}")
    
    # Process tables
    tables = con.cursor().execute("SHOW TABLES").fetchall()
    for table in tables:
        table_name = table[1]  # The second column is the table name
        # Check if the table name starts with 'STRIPE_', 'ES_' or 'ESERPARR_'
        if table_name.startswith(('ADJUSTMENTS_OIP_UNITS_TEST_TO_DELETE')):
            print(f"Processing table: {table_name}")
            try:
                ddl = con.cursor().execute(f"SELECT GET_DDL('table', '{table_name}')").fetchone()[0]
                # Write the DDL to a file
                with open(f"sql/{table_name}.sql", 'w') as f:
                    f.write(ddl)
                print(f"Successfully wrote DDL for {table_name} to file")
            except ProgrammingError as e:
                print(f"Failed to get DDL for table {table_name}: {e}")
    
    # Process views
    views = con.cursor().execute("SHOW VIEWS").fetchall()
    for view in views:
        view_name = view[1]  # The second column is the view name
        # Check if the view name starts with 'STRIPE_', 'ES_' or 'ESERPARR_'
        if view_name.startswith(('EXO_TO_ADV_STG_')):
            print(f"Processing view: {view_name}")
            try:
                ddl = con.cursor().execute(f"SELECT GET_DDL('view', '{view_name}')").fetchone()[0]
                # Write the DDL to a file
                with open(f"sql/{view_name}.sql", 'w') as f:
                    f.write(ddl)
                print(f"Successfully wrote DDL for {view_name} to file")
            except ProgrammingError as e:
                print(f"Failed to get DDL for view {view_name}: {e}")
