FROM airbyte/python-connector-base:4.0.2

WORKDIR /airbyte/integration_code
COPY . .
RUN pip install .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
