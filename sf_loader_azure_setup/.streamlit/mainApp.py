import streamlit as st

# Function to generate the OAuth URL for initiating the login flow
def generate_oauth_url():
    client_id = st.secrets["azure_ad"]["CLIENT_ID"]
    tenant_id = st.secrets["azure_ad"]["TENANT_ID"]
    redirect_uri = st.secrets["azure_ad"]["REDIRECT_URI"]
    state = "your_unique_state_value"  # Generate or define a unique state value
    scope = "https://.default"  # Define the scope of access

    oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri}&scope={scope}&state={state}&response_mode=query"
    return oauth_url

# Display the link or button in Streamlit for user to start OAuth flow
st.title("OAuth with Azure AD and Snowflake")
oauth_url = generate_oauth_url()
st.markdown(f"[Authenticate with Azure AD]({oauth_url})", unsafe_allow_html=True)


