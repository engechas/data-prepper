package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import kafka.common.BrokerEndPointNotAvailableException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.ClientDNSLookupType;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class KafkaSourceCustomConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumerFactory.class);

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private String schemaType = MessageFormat.PLAINTEXT.toString();

    public List<KafkaSourceCustomConsumer> createConsumersForTopic(final KafkaSourceConfig sourceConfig, final TopicConfig topic,
                                                                    final Buffer<Record<Event>> buffer, final PluginMetrics pluginMetrics,
                                                                    final AcknowledgementSetManager acknowledgementSetManager,
                                                                    final AtomicBoolean shutdownInProgress) {
        Properties authProperties = new Properties();
        KafkaSourceSecurityConfigurer.setAuthProperties(authProperties, sourceConfig, LOG);
        KafkaTopicMetrics topicMetrics = new KafkaTopicMetrics(topic.getName(), pluginMetrics);
        Properties consumerProperties = getConsumerProperties(sourceConfig, topic, authProperties);
        MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);

        final List<KafkaSourceCustomConsumer> consumers = new ArrayList<>();

        try {
            final int numWorkers = topic.getWorkers();
            IntStream.range(0, numWorkers).forEach(index -> {
                final KafkaConsumer kafkaConsumer;
                switch (schema) {
                    case JSON:
                        kafkaConsumer = new KafkaConsumer<String, JsonNode>(consumerProperties);
                        break;
                    case AVRO:
                        kafkaConsumer = new KafkaConsumer<String, GenericRecord>(consumerProperties);
                        break;
                    case BYTES:
                        kafkaConsumer = new KafkaConsumer<String, byte[]>(consumerProperties);
                        break;
                    case PLAINTEXT:
                    default:
                        final GlueSchemaRegistryKafkaDeserializer glueDeserializer = KafkaSourceSecurityConfigurer.getGlueSerializer(sourceConfig);
                        if (Objects.nonNull(glueDeserializer)) {
                            kafkaConsumer = new KafkaConsumer(consumerProperties, stringDeserializer, glueDeserializer);
                        } else {
                            kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
                        }
                        break;
                }
                consumers.add(new KafkaSourceCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topic,
                        schemaType, acknowledgementSetManager, topicMetrics));

            });
        } catch (Exception e) {
            if (e instanceof BrokerNotAvailableException ||
                    e instanceof BrokerEndPointNotAvailableException || e instanceof TimeoutException) {
                LOG.error("The kafka broker is not available...");
            } else {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
            }
            throw new RuntimeException(e);
        }

        return consumers;
    }

    private Properties getConsumerProperties(final KafkaSourceConfig sourceConfig, final TopicConfig topicConfig, final Properties authProperties) {
        Properties properties = (Properties)authProperties.clone();
        if (StringUtils.isNotEmpty(sourceConfig.getClientDnsLookup())) {
            ClientDNSLookupType dnsLookupType = ClientDNSLookupType.getDnsLookupType(sourceConfig.getClientDnsLookup());
            switch (dnsLookupType) {
                case USE_ALL_DNS_IPS:
                    properties.put("client.dns.lookup", ClientDNSLookupType.USE_ALL_DNS_IPS.toString());
                    break;
                case CANONICAL_BOOTSTRAP:
                    properties.put("client.dns.lookup", ClientDNSLookupType.CANONICAL_BOOTSTRAP.toString());
                    break;
                case DEFAULT:
                    properties.put("client.dns.lookup", ClientDNSLookupType.DEFAULT.toString());
                    break;
            }
        }
        setConsumerTopicProperties(properties, topicConfig);
        setSchemaRegistryProperties(sourceConfig, properties, topicConfig);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
    }

    private void setConsumerTopicProperties(final Properties properties, final TopicConfig topicConfig) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int)topicConfig.getMaxPartitionFetchBytes());
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, ((Long)topicConfig.getRetryBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, ((Long)topicConfig.getReconnectBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                ((Long)topicConfig.getCommitInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                ((Long)topicConfig.getMaxPollInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ((Long)topicConfig.getSessionTimeOut().toMillis()).intValue());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ((Long)topicConfig.getHeartBeatInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int)topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int)topicConfig.getFetchMinBytes());
    }

    private void setSchemaRegistryProperties(final KafkaSourceConfig sourceConfig, final Properties properties, final TopicConfig topicConfig) {
        SchemaConfig schemaConfig = sourceConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig)) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties, topicConfig);
            return;
        }

        if (schemaConfig.getType() == SchemaRegistryType.AWS_GLUE) {
            return;
        }

        /* else schema registry type is Confluent */
        if (StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(sourceConfig, properties);
            setPropertiesForSchemaType(sourceConfig, properties, topicConfig);
        } else {
            throw new RuntimeException("RegistryURL must be specified for confluent schema registry");
        }
    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties, final TopicConfig topicConfig) {
        MessageFormat dataFormat = topicConfig.getSerdeFormat();
        schemaType = dataFormat.toString();
        LOG.error("Setting schemaType to {}", schemaType);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        switch (dataFormat) {
            case JSON:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
                break;
            case BYTES:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
                break;
            default:
            case PLAINTEXT:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class);
                break;
        }
    }

    private void setPropertiesForSchemaRegistryConnectivity(final KafkaSourceConfig sourceConfig, final Properties properties) {
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        String schemaRegistryApiKey = sourceConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication for schema registry
        if ("USER_INFO".equalsIgnoreCase(sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource())
                && authConfig.getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("schema.registry.basic.auth.user.info", schemaBasicAuthUserInfo);
            properties.put("basic.auth.credentials.source", "USER_INFO");
        }

        if (authConfig != null && authConfig.getSaslAuthConfig() != null) {
            PlainTextAuthConfig plainTextAuthConfig = authConfig.getSaslAuthConfig().getPlainTextAuthConfig();
            OAuthConfig oAuthConfig = authConfig.getSaslAuthConfig().getOAuthConfig();
            if (oAuthConfig != null) {
                properties.put("sasl.mechanism", oAuthConfig.getOauthSaslMechanism());
                properties.put("security.protocol", oAuthConfig.getOauthSecurityProtocol());
            }
        }
    }

    private void setPropertiesForSchemaType(final KafkaSourceConfig sourceConfig, final Properties properties, final TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl(sourceConfig));
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        final CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(properties.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(topic.getName() + "-value",
                    sourceConfig.getSchemaConfig().getVersion()).getSchemaType();
        } catch (IOException | RestClientException e) {
            LOG.error("Failed to connect to the schema registry...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private String getSchemaRegistryUrl(final KafkaSourceConfig sourceConfig) {
        return sourceConfig.getSchemaConfig().getRegistryURL();
    }
}
