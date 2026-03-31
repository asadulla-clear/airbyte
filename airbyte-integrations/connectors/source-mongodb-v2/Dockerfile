# Stage 1: Build the Java connector
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /airbyte
# Copy the entire Airbyte submodule to satisfy Gradle dependencies
# Note: The build context should be the root of the airbyte submodule (prism-connectors/airbyte)
COPY . .

# Build only the MongoDB V2 connector
RUN ./gradlew :airbyte-integrations:connectors:source-mongodb-v2:assemble -x :airbyte-integrations:connectors:source-mongodb-v2:dockerBuildx --no-daemon

# Stage 2: Create the final image
# Base image from metadata.yaml
FROM airbyte/java-connector-base:2.0.1

WORKDIR /airbyte
# Copy the distribution tarball from the builder
COPY --from=builder /airbyte/airbyte-integrations/connectors/source-mongodb-v2/build/distributions/*.tar /airbyte/
# Extract the tarball and set up the directory structure
RUN tar -xvf /airbyte/*.tar --strip-components=1 -C /airbyte && rm /airbyte/*.tar

ENV AIRBYTE_ENTRYPOINT="/airbyte/bin/source-mongodb-v2"
ENTRYPOINT ["/airbyte/bin/source-mongodb-v2"]
