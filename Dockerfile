FROM python:3.11-slim
COPY . /snowloader-app
WORKDIR /snowloader-app
RUN pip install -r requirements.txt
EXPOSE 80
RUN cp .streamlit/config.toml .streamlit/config.toml
RUN cp .streamlit/credentials.toml .streamlit/credentials.toml
WORKDIR /snowloader-app
ENTRYPOINT ["streamlit", "run"]
CMD ["snowloader_app.py"]