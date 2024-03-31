# snowloader-app/Dockerfile

FROM python:3.9-slim

# Copy the application source code and Streamlit configuration files to the container
WORKDIR /snowloader-app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/roshan-abady/snowloader-app.git
# Install Python dependencies
RUN pip3 install -r requirements.txt

# Expose port 8501 for local development; cloud providers will use their own port.
EXPOSE 80
EXPOSE 8501

# Disable Streamlit's onboard email prompt and ensure it can run in headless environments.
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=False
ENV STREAMLIT_SERVER_HEADLESS=true

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Define the entrypoint and default command to run the Streamlit application, adapting to the PORT environment variable.
ENTRYPOINT ["sh", "-c", "streamlit run snowloader_app.py --server.port=$PORT --server.address=0.0.0.0"]
