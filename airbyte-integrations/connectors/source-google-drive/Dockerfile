FROM airbyte/python-connector-base:4.1.0
WORKDIR /airbyte/integration_code
COPY . .
RUN pip install .
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
