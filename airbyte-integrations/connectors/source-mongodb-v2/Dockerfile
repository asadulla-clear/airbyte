# Stage 1: Build the Java connector
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /airbyte
# Copy the entire Airbyte submodule to satisfy Gradle dependencies
# Note: The build context should be the root of the airbyte submodule (prism-connectors/airbyte)
COPY . .

# Build only the MongoDB V2 connector, ensuring all resources like spec.json are bundled
RUN ./gradlew :airbyte-integrations:connectors:source-mongodb-v2:build \
    -x test \
    -x :airbyte-integrations:connectors:source-mongodb-v2:docker \
    -x :airbyte-integrations:connectors:source-mongodb-v2:dockerBuildx \
    -x :airbyte-integrations:connectors:source-mongodb-v2:copyArtifacts \
    --no-daemon

# Stage 2: Create the final image
# Base image from metadata.yaml
FROM airbyte/java-connector-base:2.0.1

WORKDIR /airbyte
# Copy the distribution tarball from the builder
COPY --from=builder /airbyte/airbyte-integrations/connectors/source-mongodb-v2/build/distributions/airbyte-app.tar /airbyte/
# Extract the tarball and set up the directory structure
RUN tar -xvf /airbyte/airbyte-app.tar --strip-components=1 -C /airbyte && rm /airbyte/airbyte-app.tar

ENV AIRBYTE_ENTRYPOINT="/airbyte/bin/source-mongodb-v2"
ENTRYPOINT ["/airbyte/bin/source-mongodb-v2"]
