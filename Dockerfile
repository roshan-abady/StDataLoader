# snowloader-app/Dockerfile

FROM python:3.9-slim

# Copy the application source code and Streamlit configuration files to the container
COPY . /snowloader-app
WORKDIR /snowloader-app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Streamlit runs on port 8501 by default, but we'll use an environment variable for Azure to specify the port.
EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Disable Streamlit's onboard email prompt and set the server to listen on the port specified by the PORT environment variable.
# This variable will be automatically used by Streamlit if it's set, otherwise we default to 8501.
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=False
ENV PORT=8501

# Define the entrypoint and default command to run the Streamlit application.
# Use `sh -c` to ensure the PORT environment variable is correctly used.
ENTRYPOINT ["streamlit", "run", "snowloader_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
