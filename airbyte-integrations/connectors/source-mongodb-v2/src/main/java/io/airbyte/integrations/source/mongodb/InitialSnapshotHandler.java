/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.cdk.integrations.source.relationaldb.state.SourceStateIterator;
import io.airbyte.cdk.integrations.source.relationaldb.state.StateEmitFrequency;
import io.airbyte.cdk.integrations.source.relationaldb.streamstatus.StreamStatusTraceEmitterIterator;
import io.airbyte.commons.exceptions.ConfigErrorException;
import io.airbyte.commons.stream.AirbyteStreamStatusHolder;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.mongodb.MongoUtil.CollectionStatistics;
import io.airbyte.integrations.source.mongodb.state.IdType;
import io.airbyte.integrations.source.mongodb.state.MongoDbStateManager;
import io.airbyte.integrations.source.mongodb.state.MongoDbStreamState;
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.*;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves iterators used for the initial snapshot
 */
public class InitialSnapshotHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(InitialSnapshotHandler.class);

  /**
   * For each given stream configured as incremental sync it will output an iterator that will
   * retrieve documents from the given database. Each iterator will start after the last checkpointed
   * document, if any, or from the beginning of the stream otherwise.
   */
  public List<AutoCloseableIterator<AirbyteMessage>> getIterators(
                                                                  final List<ConfiguredAirbyteStream> streams,
                                                                  final MongoDbStateManager stateManager,
                                                                  final MongoDatabase database,
                                                                  final MongoDbSourceConfig config,
                                                                  final boolean decorateWithStartedStatus,
                                                                  final boolean decorateWithCompletedStatus,
                                                                  final Instant emittedAt,
                                                                  final Optional<Duration> cdcInitialLoadTimeout) {
    final boolean isEnforceSchema = config.getEnforceSchema();
    final var checkpointInterval = config.getCheckpointInterval();
    final String MULTIPLE_ID_TYPES_ANALYTICS_MESSAGE_KEY = "db-sources-mongo-multiple-id-types";

    return streams
        .stream()
        .filter(airbyteStream -> airbyteStream.getStream().getNamespace().equals(database.getName()))
        .map(airbyteStream -> {
          String streamName = airbyteStream.getStream().getName();
          final String namespace = airbyteStream.getStream().getNamespace();
          
          String collectionName = streamName;
          String shardValue = null;
          String shardingField = config.getShardingField();
          
          if (shardingField != null && !shardingField.isEmpty()) {
            Set<String> authorizedCollections = MongoUtil.getAuthorizedCollections(database);
            if (!authorizedCollections.contains(streamName)) {
                for (String authColl : authorizedCollections) {
                    if (streamName.startsWith(authColl + "_")) {
                        if (shardValue == null || authColl.length() > collectionName.length()) {
                            collectionName = authColl;
                            // The suffix is the SANITIZED shard value
                            String sanitizedSuffix = streamName.substring(authColl.length() + 1);
                            
                            // To find the ORIGINAL shard value, we query the distinct values and pick the one that matches after sanitization
                            List<Object> distinctValues = new ArrayList<>();
                            try {
                                database.getCollection(authColl).distinct(shardingField, String.class).into(distinctValues);
                            } catch (Exception e) {
                                LOGGER.warn("Failed to get distinct values for {} in sync: {}", authColl, e.getMessage());
                            }
                            
                            for (Object val : distinctValues) {
                                String valStr = Objects.toString(val);
                                if (val instanceof org.bson.types.Binary) {
                                    valStr = java.util.UUID.nameUUIDFromBytes(((org.bson.types.Binary) val).getData()).toString();
                                }
                                
                                if (val != null && valStr.replaceAll("[^a-zA-Z0-9]", "_").equals(sanitizedSuffix)) {
                                    shardValue = valStr;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
          }

          final String resolvedCollectionName = collectionName;
          final String resolvedShardValue = shardValue;
          final var collection = database.getCollection(resolvedCollectionName);
          final Bson shardingFilter = resolvedShardValue != null ? Filters.eq(shardingField, resolvedShardValue) : null;
          
          final var fields = Projections.fields(Projections.include(CatalogHelpers.getTopLevelFieldNames(airbyteStream).stream().toList()));
          final var idTypes = aggregateIdField(collection);
          if (idTypes.size() > 1) {
            LOGGER.warn("The _id fields in this collection are not consistently typed, which may lead to data loss (collection = {}).",
                resolvedCollectionName);
            AirbyteTraceMessageUtility
                .emitAnalyticsTrace(new AirbyteAnalyticsTraceMessage().withType(MULTIPLE_ID_TYPES_ANALYTICS_MESSAGE_KEY).withValue("1"));
          }

          idTypes.stream().findFirst().ifPresent(idType -> {
            if (IdType.findByBsonType(idType).isEmpty()) {
              throw new ConfigErrorException("Only _id fields with the following types are currently supported: " + IdType.SUPPORTED
                  + " (collection = " + resolvedCollectionName + ").");
            }
          });

          // find the existing state, if there is one, for this stream
          final Optional<MongoDbStreamState> existingState =
              stateManager.getStreamState(airbyteStream.getStream().getName(), airbyteStream.getStream().getNamespace());

          final Optional<CollectionStatistics> collectionStatistics = MongoUtil.getCollectionStatistics(database, airbyteStream);
          final var recordIterator = new MongoDbInitialLoadRecordIterator(collection, fields, existingState, isEnforceSchema,
              MongoUtil.getChunkSizeForCollection(collectionStatistics, airbyteStream), shardingFilter, emittedAt, cdcInitialLoadTimeout);
          final var stateIterator =
              new SourceStateIterator<>(recordIterator, airbyteStream, stateManager, new StateEmitFrequency(checkpointInterval,
                  MongoConstants.CHECKPOINT_DURATION));
          final var iterator = AutoCloseableIterators.fromIterator(stateIterator, recordIterator::close, null);

          List<AutoCloseableIterator<AirbyteMessage>> itList = Stream.of(iterator).collect(Collectors.toList());
          if (decorateWithStartedStatus) {
            itList.addFirst(new StreamStatusTraceEmitterIterator(
                new AirbyteStreamStatusHolder(new io.airbyte.protocol.models.AirbyteStreamNameNamespacePair(collectionName, namespace),
                    AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED)));
          }

          if (decorateWithCompletedStatus) {
            itList.addLast(new StreamStatusTraceEmitterIterator(
                new AirbyteStreamStatusHolder(new io.airbyte.protocol.models.AirbyteStreamNameNamespacePair(collectionName, namespace),
                    AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)));
          }
          return (itList.size() == 1) ? iterator : AutoCloseableIterators.concatWithEagerClose(itList);
        })
        .toList();
  }

  /**
   * Returns a list of types (as strings) that the _id field has for the provided collection.
   *
   * @param collection Collection to aggregate the _id types of.
   * @return List of bson types (as strings) that the _id field contains.
   */
  private List<String> aggregateIdField(final MongoCollection<Document> collection) {
    final List<String> idTypes = new ArrayList<>();
    /*
     * Sanity check that all ID_FIELD values are of the same type for this collection.
     * db.collection.aggregate([{ $group : { _id : { $type : "$_id" }, count : { $sum : 1 } } }])
     */
    collection.aggregate(List.of(
        Aggregates.group(
            new Document(MongoConstants.ID_FIELD, new Document("$type", "$_id")),
            Accumulators.sum("count", 1))))
        .forEach(document -> {
          // the document will be in the structure of
          // {"_id": {"_id": "[TYPE]"}, "count": [COUNT]}
          // where [TYPE] is the bson type (objectId, string, etc.) and [COUNT] is the number of documents of
          // that type
          final Document innerDocument = document.get(MongoConstants.ID_FIELD, Document.class);
          idTypes.add(innerDocument.get(MongoConstants.ID_FIELD).toString());
        });

    return idTypes;
  }

}
