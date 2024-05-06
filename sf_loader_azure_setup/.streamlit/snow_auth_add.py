import streamlit as st
import requests
import jwt
from jwt.algorithms import RSAAlgorithm
import json

# Securely fetching your credentials from the secrets.toml file
CLIENT_ID = st.secrets["azure_ad"]["CLIENT_ID"]
TENANT_ID = st.secrets["azure_ad"]["TENANT_ID"]
REDIRECT_URI = st.secrets["azure_ad"]["REDIRECT_URI"]

# Function to fetch Microsoft's public keys for signature validation
def get_microsoft_public_keys(tenant_id):
    open_id_config_url = f"https://login.microsoftonline.com/{tenant_id}/v2.0/.well-known/openid-configuration"
    jwks_uri = requests.get(open_id_config_url).json()['jwks_uri']
    jwks_keys = requests.get(jwks_uri).json()['keys']
    print(jwks_keys)
    print(jwks_uri)
    print(open_id_config_url)
    return {key['kid']: RSAAlgorithm.from_jwk(json.dumps(key)) for key in jwks_keys}

# Function to validate an ID token
def validate_id_token(id_token):
    public_keys = get_microsoft_public_keys(TENANT_ID)
    header = jwt.get_unverified_header(id_token)
    key = public_keys[header['kid']]
    decoded_token = jwt.decode(id_token, key=key, algorithms=['RS256'], audience=CLIENT_ID)
    print(decoded_token)
    print(key)
    print(public_keys)
    print(header)
    print(id_token)
    return decoded_token

# Example usage in your Streamlit app
# This is a placeholder for where you would actually implement the OAuth flow and receive the ID token
# For example, this could be part of a login callback route or similar mechanism
# id_token = "YOUR_ID_TOKEN_HERE"
# decoded_token = validate_id_token(id_token)
# st.write("Decoded ID Token:", decoded_token)
# Construct the OAuth URL (simplified, adjust scopes and response_mode as needed)
oauth_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/authorize?client_id={CLIENT_ID}&response_type=id_token&redirect_uri={REDIRECT_URI}&response_mode=fragment&scope=openid profile email&state=12345&nonce=678910"

if 'id_token' not in st.session_state:
    # Embed a button in Streamlit to initiate the OAuth flow
    st.markdown(f'<a href="{oauth_url}" target="_blank">Login with Azure AD</a>', unsafe_allow_html=True)

    # Placeholder for receiving the ID token - in a real scenario, you would need to handle this differently
    id_token = st.text_input("Paste the ID token here:", "")

    if id_token:
        st.session_state['id_token'] = id_token
        # Validate the ID token
        try:
            decoded_token = validate_id_token(id_token, CLIENT_ID, TENANT_ID)
            st.success("Token is valid!")
            # Display decoded token or proceed with application logic
            st.json(decoded_token)
        except Exception as e:
            st.error(f"Token validation failed: {str(e)}")
else:
    st.success("Already logged in!")
    # Proceed with application logic using the validated token