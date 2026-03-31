# Final Stage: Create the final image
# We build the Java artifacts on the host (GitHub runner) for speed and cache support.
FROM airbyte/java-connector-base:2.0.1

WORKDIR /airbyte

# Copy the distribution tarball from the host build directory
# The path is relative to the build context root (prism-connectors/airbyte)
COPY airbyte-integrations/connectors/source-mongodb-v2/build/distributions/airbyte-app.tar /airbyte/

# Extract the tarball and set up the directory structure
RUN tar -xvf /airbyte/airbyte-app.tar --strip-components=1 -C /airbyte && rm /airbyte/airbyte-app.tar

ENV AIRBYTE_ENTRYPOINT="/airbyte/bin/source-mongodb-v2"
ENTRYPOINT ["/airbyte/bin/source-mongodb-v2"]
