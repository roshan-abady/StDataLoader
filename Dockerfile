# snowloader-app/Dockerfile

FROM python:3.9-slim

# Copy the application source code and Streamlit configuration files to the container
WORKDIR /snowloader-app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    unzip \
    wget \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    npm \
    chromium \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN wget https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip
RUN unzip chromedriver_linux64.zip
RUN mv chromedriver /usr/bin/chromedriver
RUN chown root:root /usr/bin/chromedriver
RUN chmod +x /usr/bin/chromedriver

COPY . .
COPY .streamlit/config.toml /snowloader-app/.streamlit/config.toml
COPY .streamlit/credentials.toml /snowloader-app/.streamlit/credentials.toml 
RUN pip install --upgrade pip
RUN pip install snowflake-connector-python[pandas]
# Install Python dependencies
RUN pip install -r requirements.txt
# Install Selenium
RUN pip install selenium
# Expose port 8501 for local development; cloud providers will use their own port.
EXPOSE 8501
EXPOSE 80
EXPOSE 443
EXPOSE 8080

# Disable Streamlit's onboard email prompt and ensure it can run in headless environments.
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=False
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_UNSAFE_ALLOW_HTML=true

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Define the entrypoint and default command to run the Streamlit application, adapting to the PORT environment variable.
ENTRYPOINT ["sh", "-c", "streamlit run snowloader_app.py --server.port=8501 --server.address=0.0.0.0"]
