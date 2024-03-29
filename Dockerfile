FROM python:3.11-slim
COPY . /snowloader-app
WORKDIR /snowloader-app
RUN pip install -r requirements.txt
EXPOSE 80
COPY .streamlit/config.toml .streamlit/config.toml
COPY .streamlit/credentials.toml .streamlit/credentials.toml
WORKDIR /snowloader-app
ENTRYPOINT ["streamlit", "run"]
CMD ["snowloader_app.py"]