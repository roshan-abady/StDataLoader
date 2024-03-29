FROM python:3.11-slim
COPY . /snowloader-app
WORKDIR /snowloader-app
RUN pip install -r requirements.txt
EXPOSE 80
RUN cp config.toml config.toml
RUN cp credentials.toml credentials.toml
WORKDIR /snowloader-app
ENTRYPOINT ["streamlit", "run"]
CMD ["snowloader_app.py"]