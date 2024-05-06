import getpass
import toml
from snowflake.snowpark import Session

# Read secrets from secrets.toml file
secrets = toml.load(".streamlit/secrets.toml")

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

def create_snowflake_session(config, access_token=None):
    """
    Create a Snowflake session using the provided configuration.

    Args:
        config (dict): A dictionary containing the Snowflake configuration.
        access_token (str, optional): An optional OAuth access token for authentication.

    Returns:
        Session: A Snowflake session.
    """
    return Session.builder.configs(config).create()
