/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerProperties;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * * This is static property configurer for related information given in pipeline.yml
 */
public class SinkPropertyConfigurer {

    private static final Logger LOG = LoggerFactory.getLogger(SinkPropertyConfigurer.class);

    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String KEY_SERIALIZER = "key.serializer";

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private static final String REGISTRY_URL = "schema.registry.url";

    private static final String REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    private static final String CREDENTIALS_SOURCE = "basic.auth.credentials.source";

    public static final String BUFFER_MEMORY = "buffer.memory";

    public static final String COMPRESSION_TYPE = "compression.type";

    public static final String RETRIES = "retries";

    public static final String BATCH_SIZE = "batch.size";

    public static final String CLIENT_DNS_LOOKUP = "client.dns.lookup";

    public static final String CLIENT_ID = "client.id";

    public static final String CONNECTIONS_MAX_IDLE_MS = "connections.max.idle.ms";

    public static final String DELIVERY_TIMEOUT_MS = "delivery.timeout.ms";

    public static final String LINGER_MS = "linger.ms";

    public static final String MAX_BLOCK_MS = "max.block.ms";

    public static final String MAX_REQUEST_SIZE = "max.request.size";

    public static final String PARTITIONER_CLASS = "partitioner.class";

    public static final String PARTITIONER_IGNORE_KEYS = "partitioner.ignore.keys";

    public static final String RECEIVE_BUFFER_BYTES = "receive.buffer.bytes";

    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";

    public static final String SEND_BUFFER_BYTES = "send.buffer.bytes";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = "socket.connection.setup.timeout.max.ms";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS = "socket.connection.setup.timeout.ms";

    public static final String ACKS = "acks";

    public static final String ENABLE_IDEMPOTENCE = "enable.idempotence";

    public static final String INTERCEPTOR_CLASSES = "interceptor.classes";

    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";

    public static final String METADATA_MAX_AGE_MS = "metadata.max.age.ms";

    public static final String METADATA_MAX_IDLE_MS = "metadata.max.idle.ms";

    public static final String METRIC_REPORTERS = "metric.reporters";

    public static final String METRICS_NUM_SAMPLES = "metrics.num.samples";

    public static final String METRICS_RECORDING_LEVEL = "metrics.recording.level";

    public static final String METRICS_SAMPLE_WINDOW_MS = "metrics.sample.window.ms";

    public static final String PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE = "partitioner.adaptive.partitioning.enable";

    public static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS = "partitioner.availability.timeout.ms";

    public static final String RECONNECT_BACKOFF_MAX_MS = "reconnect.backoff.max.ms";

    public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    private static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";

    private static final String SASL_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";

    private static final String SASL_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";

    private static final String OAUTH_JAASCONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='"
            + "%s" + "' clientSecret='" + "%s" + "' scope='" + "%s" + "' OAUTH_LOGIN_SERVER='" + "%s" +
            "' OAUTH_LOGIN_ENDPOINT='" + "%s" + "' OAUT_LOGIN_GRANT_TYPE=" + "%s" +
            " OAUTH_LOGIN_SCOPE=%s OAUTH_AUTHORIZATION='Basic " + "%s" + "';";

    private static final String INSTROSPECT_SERVER_PROPERTIES = " OAUTH_INTROSPECT_SERVER='"
            + "%s" + "' OAUTH_INTROSPECT_ENDPOINT='" + "%s" + "' " +
            "OAUTH_INTROSPECT_AUTHORIZATION='Basic " + "%s";

    private static final int MAX_KAFKA_CLIENT_RETRIES = 360; // for one hour every 10 seconds

    private static AwsCredentialsProvider credentialsProvider;


    public static Properties getProducerProperties(final KafkaSinkConfig kafkaSinkConfig) {
        final Properties properties = new Properties();

        setCommonServerProperties(properties, kafkaSinkConfig);

        setPropertiesForSerializer(properties, kafkaSinkConfig.getSerdeFormat());

        if (kafkaSinkConfig.getSchemaConfig() != null) {
            setSchemaProps(kafkaSinkConfig.getSerdeFormat(), kafkaSinkConfig.getSchemaConfig(), properties);
        }
        if (kafkaSinkConfig.getKafkaProducerProperties() != null) {
            setPropertiesProviderByKafkaProducer(kafkaSinkConfig.getKafkaProducerProperties(), properties);
        }

        setAuthProperties(properties, kafkaSinkConfig, LOG);

        return properties;
    }

    private static void setPlainTextAuthProperties(Properties properties, final PlainTextAuthConfig plainTextAuthConfig, EncryptionType encryptionType) {
        String username = plainTextAuthConfig.getUsername();
        String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, "PLAIN");
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (encryptionType == EncryptionType.NONE) {
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        } else { // EncryptionType.SSL
            properties.put(SECURITY_PROTOCOL, "SASL_SSL");
        }
    }

    public static void setOauthProperties(final KafkaSinkConfig kafkaSinkConfig,
                                          final Properties properties) {
        final OAuthConfig oAuthConfig = kafkaSinkConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
        final String oauthClientId = oAuthConfig.getOauthClientId();
        final String oauthClientSecret = oAuthConfig.getOauthClientSecret();
        final String oauthLoginServer = oAuthConfig.getOauthLoginServer();
        final String oauthLoginEndpoint = oAuthConfig.getOauthLoginEndpoint();
        final String oauthLoginGrantType = oAuthConfig.getOauthLoginGrantType();
        final String oauthLoginScope = oAuthConfig.getOauthLoginScope();
        final String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        final String oauthIntrospectEndpoint = oAuthConfig.getOauthIntrospectEndpoint();
        final String tokenEndPointURL = oAuthConfig.getOauthTokenEndpointURL();
        final String saslMechanism = oAuthConfig.getOauthSaslMechanism();
        final String securityProtocol = oAuthConfig.getOauthSecurityProtocol();
        final String loginCallBackHandler = oAuthConfig.getOauthSaslLoginCallbackHandlerClass();
        final String oauthJwksEndpointURL = oAuthConfig.getOauthJwksEndpointURL();
        final String introspectServer = oAuthConfig.getOauthIntrospectServer();


        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SECURITY_PROTOCOL, securityProtocol);
        properties.put(SASL_TOKEN_ENDPOINT_URL, tokenEndPointURL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, loginCallBackHandler);


        if (oauthJwksEndpointURL != null && !oauthJwksEndpointURL.isEmpty() && !oauthJwksEndpointURL.isBlank()) {
            properties.put(SASL_JWKS_ENDPOINT_URL, oauthJwksEndpointURL);
        }

        String instrospect_properties = "";
        if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospect_properties = String.format(INSTROSPECT_SERVER_PROPERTIES, introspectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }

        String jass_config = String.format(OAUTH_JAASCONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospect_properties);

        if ("USER_INFO".equalsIgnoreCase(kafkaSinkConfig.getSchemaConfig().getBasicAuthCredentialsSource())) {
            final String apiKey = kafkaSinkConfig.getSchemaConfig().getSchemaRegistryApiKey();
            final String apiSecret = kafkaSinkConfig.getSchemaConfig().getSchemaRegistryApiSecret();
            final String extensionLogicalCluster = oAuthConfig.getExtensionLogicalCluster();
            final String extensionIdentityPoolId = oAuthConfig.getExtensionIdentityPoolId();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
            properties.put("basic.auth.credentials.source", "USER_INFO");
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=  " + " \"%s\";";
            jass_config = jass_config.replace(";", " ");
            jass_config += String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }
        properties.put(SASL_JAAS_CONFIG, jass_config);
    }

    public static void setAwsIamAuthProperties(Properties properties, final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig) {
        properties.put(SECURITY_PROTOCOL, "SASL_SSL");
        properties.put(SASL_MECHANISM, "AWS_MSK_IAM");
        properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            properties.put(SASL_JAAS_CONFIG,
                    "software.amazon.msk.auth.iam.IAMLoginModule required " +
                            "awsRoleArn=\"" + awsConfig.getStsRoleArn() +
                            "\" awsStsRegion=\"" + awsConfig.getRegion() + "\";");
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put(SASL_JAAS_CONFIG,
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    public static String getBootStrapServersForMsk(final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig, final Logger LOG) {
        LOG.error("GOT here");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(credentialsProvider)
                    .build();
            credentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                            AssumeRoleRequest
                                    .builder()
                                    .roleArn(awsConfig.getStsRoleArn())
                                    .roleSessionName(sessionName)
                                    .build()
                    ).build();
        } else if (awsIamAuthConfig != AwsIamAuthConfig.DEFAULT) {
            throw new RuntimeException("Unknown AWS IAM auth mode");
        }
        final AwsConfig.AwsMskConfig awsMskConfig = awsConfig.getAwsMskConfig();
        KafkaClient kafkaClient = KafkaClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsConfig.getRegion()))
                .build();
        final GetBootstrapBrokersRequest request =
                GetBootstrapBrokersRequest
                        .builder()
                        .clusterArn(awsMskConfig.getArn())
                        .build();

        int numRetries = 0;
        boolean retryable;
        GetBootstrapBrokersResponse result = null;
        do {
            retryable = false;
            try {
                result = kafkaClient.getBootstrapBrokers(request);
            } catch (KafkaException | StsException e) {
                LOG.info("Failed to get bootstrap server information from MSK. Will try every 10 seconds for {} seconds", 10*MAX_KAFKA_CLIENT_RETRIES, e);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException exp) {}
                retryable = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to get bootstrap server information from MSK.", e);
            }
        } while (retryable && numRetries++ < MAX_KAFKA_CLIENT_RETRIES);
        if (Objects.isNull(result)) {
            throw new RuntimeException("Failed to get bootstrap server information from MSK after trying multiple times with retryable exceptions.");
        }
        switch (awsMskConfig.getBrokerConnectionType()) {
            case PUBLIC:
                return result.bootstrapBrokerStringPublicSaslIam();
            case MULTI_VPC:
                return result.bootstrapBrokerStringVpcConnectivitySaslIam();
            default:
            case SINGLE_VPC:
                return result.bootstrapBrokerStringSaslIam();
        }
    }

    public static void setAuthProperties(Properties properties, final KafkaSinkConfig sinkConfig, final Logger LOG) {
        final AwsConfig awsConfig = sinkConfig.getAwsConfig();
        final AuthConfig authConfig = sinkConfig.getAuthConfig();
        final KafkaSinkConfig.EncryptionConfig encryptionConfig = sinkConfig.getEncryptionConfig();
        final EncryptionType encryptionType = encryptionConfig.getType();

        credentialsProvider = DefaultCredentialsProvider.create();

        String bootstrapServers = sinkConfig.getBootStrapServers();
        AwsIamAuthConfig awsIamAuthConfig = null;
        if (Objects.nonNull(authConfig)) {
            AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig)) {
                awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();

                if (Objects.nonNull(awsIamAuthConfig)) {
                    if (encryptionType == EncryptionType.NONE) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    if (Objects.isNull(awsConfig)) {
                        throw new RuntimeException("AWS Config is not specified");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                    bootstrapServers = getBootStrapServersForMsk(awsIamAuthConfig, awsConfig, LOG);
                } else if (Objects.nonNull(saslAuthConfig.getOAuthConfig())) {
                    setOauthProperties(sinkConfig, properties);
                } else if (Objects.nonNull(plainTextAuthConfig)) {
                    setPlainTextAuthProperties(properties, plainTextAuthConfig, encryptionType);
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            }
            if (encryptionConfig.getInsecure()) {
                properties.put("ssl.engine.factory.class", InsecureSslEngineFactory.class);
            }
        }
        if (Objects.isNull(authConfig) || Objects.isNull(authConfig.getSaslAuthConfig())) {
            if (encryptionType == EncryptionType.SSL) {
                properties.put(SECURITY_PROTOCOL, "SSL");
            }
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private static void setCommonServerProperties(final Properties properties, final KafkaSinkConfig kafkaSinkConfig) {
        properties.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
    }

    private static void setPropertiesForSerializer(final Properties properties, final String serdeFormat) {
        properties.put(KEY_SERIALIZER, StringSerializer.class.getName());
        if (serdeFormat.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(VALUE_SERIALIZER, JsonSerializer.class.getName());
        } else if (serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(VALUE_SERIALIZER, KafkaAvroSerializer.class.getName());
        } else if (serdeFormat.equalsIgnoreCase(MessageFormat.BYTES.toString())) {
            properties.put(VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        } else {
            properties.put(VALUE_SERIALIZER, StringSerializer.class.getName());
        }
    }

    private static void validateForRegistryURL(final String serdeFormat, final SchemaConfig schemaConfig) {

        if (serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            if (schemaConfig == null || schemaConfig.getRegistryURL() == null ||
                    schemaConfig.getRegistryURL().isBlank() || schemaConfig.getRegistryURL().isEmpty()) {
                throw new RuntimeException("Schema registry is mandatory when serde type is avro");
            }
        }
        if (serdeFormat.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            if (schemaConfig != null &&
                    schemaConfig.getRegistryURL() != null) {
                throw new RuntimeException("Schema registry is not required for type plaintext");
            }
        }
    }

    public static void setSchemaProps(final String serdeFormat, final SchemaConfig schemaConfig, final Properties properties) {
        validateForRegistryURL(serdeFormat, schemaConfig);
        final String registryURL = schemaConfig != null ? schemaConfig.getRegistryURL() : null;
        if (registryURL != null && !registryURL.isEmpty()) {
            properties.put(REGISTRY_URL, registryURL);
        }
        setSchemaCredentialsConfig(schemaConfig, properties);
    }

    public static void setSchemaCredentialsConfig(final SchemaConfig schemaConfig,final Properties properties) {
        if (!ObjectUtils.isEmpty(schemaConfig.getBasicAuthCredentialsSource())) {
            properties.put(CREDENTIALS_SOURCE, schemaConfig.getBasicAuthCredentialsSource());
        }
        if (!ObjectUtils.isEmpty(schemaConfig.getSchemaRegistryApiKey()) && !(ObjectUtils.isEmpty(schemaConfig.getSchemaRegistryApiSecret()))) {
            final String apiKey = schemaConfig.getSchemaRegistryApiKey();
            final String apiSecret = schemaConfig.getSchemaRegistryApiSecret();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
        }
    }

    private static void setPropertiesProviderByKafkaProducer(final KafkaProducerProperties producerProperties, final
    Properties properties) {

        if (producerProperties.getBufferMemory() != null) {
            properties.put(BUFFER_MEMORY, ByteCount.parse(producerProperties.getBufferMemory()).getBytes());
        }
        if (producerProperties.getCompressionType() != null) {
            properties.put(COMPRESSION_TYPE, producerProperties.getCompressionType());
        }
        properties.put(RETRIES, producerProperties.getRetries());

        if (producerProperties.getBatchSize() > 0) {
            properties.put(BATCH_SIZE, producerProperties.getBatchSize());
        }
        if (producerProperties.getClientDnsLookup() != null) {
            properties.put(CLIENT_DNS_LOOKUP, producerProperties.getClientDnsLookup());
        }
        if (producerProperties.getClientId() != null) {
            properties.put(CLIENT_ID, producerProperties.getClientId());
        }
        if (producerProperties.getConnectionsMaxIdleMs() > 0) {
            properties.put(CONNECTIONS_MAX_IDLE_MS, producerProperties.getConnectionsMaxIdleMs());
        }
        if (producerProperties.getDeliveryTimeoutMs() > 0) {
            properties.put(DELIVERY_TIMEOUT_MS, producerProperties.getDeliveryTimeoutMs().intValue());
        }
        if (producerProperties.getLingerMs() > 0) {
            properties.put(LINGER_MS, (producerProperties.getLingerMs()));
        }
        if (producerProperties.getMaxBlockMs() > 0) {
            properties.put(MAX_BLOCK_MS, producerProperties.getMaxBlockMs());
        }
        if (producerProperties.getMaxRequestSize() > 0) {
            properties.put(MAX_REQUEST_SIZE, producerProperties.getMaxRequestSize());
        }
        if (producerProperties.getPartitionerClass() != null) {
            properties.put(PARTITIONER_CLASS, producerProperties.getPartitionerClass().getName());
        }
        if (producerProperties.getPartitionerIgnoreKeys() != null) {
            properties.put(PARTITIONER_IGNORE_KEYS, producerProperties.getPartitionerIgnoreKeys());
        }
        if (producerProperties.getReceiveBufferBytes() != null) {
            final Long receiveBufferBytes = ByteCount.parse(producerProperties.getReceiveBufferBytes()).getBytes();
            properties.put(RECEIVE_BUFFER_BYTES, receiveBufferBytes.intValue());
        }
        if (producerProperties.getRequestTimeoutMs() > 0) {
            properties.put(REQUEST_TIMEOUT_MS, producerProperties.getRequestTimeoutMs().intValue());
        }
        if (producerProperties.getSendBufferBytes() != null) {
            final Long sendBufferBytes = ByteCount.parse(producerProperties.getSendBufferBytes()).getBytes();
            properties.put(SEND_BUFFER_BYTES, sendBufferBytes.intValue());
        }
        if (producerProperties.getSocketConnectionSetupMaxTimeout() > 0) {
            properties.put(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, producerProperties.getSocketConnectionSetupMaxTimeout());
        }
        if (producerProperties.getSocketConnectionSetupTimeout() > 0) {
            properties.put(SOCKET_CONNECTION_SETUP_TIMEOUT_MS, producerProperties.getSocketConnectionSetupTimeout());
        }
        if (producerProperties.getAcks() != null) {
            properties.put(ACKS, producerProperties.getAcks());
        }
        if (producerProperties.getEnableIdempotence() != null) {
            properties.put(ENABLE_IDEMPOTENCE, producerProperties.getEnableIdempotence());
        }


        List<String> interceptorClasses = producerProperties.getInterceptorClasses();
        if (interceptorClasses != null && !interceptorClasses.isEmpty()) {
            properties.put(INTERCEPTOR_CLASSES, String.join(",", interceptorClasses));
        }

        if (producerProperties.getMaxInFlightRequestsPerConnection() > 0) {
            properties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, producerProperties.getMaxInFlightRequestsPerConnection());
        }

        if (producerProperties.getMetadataMaxAgeMs() > 0) {
            properties.put(METADATA_MAX_AGE_MS, producerProperties.getMetadataMaxAgeMs());
        }

        if (producerProperties.getMetadataMaxIdleMs() > 0) {
            properties.put(METADATA_MAX_IDLE_MS, producerProperties.getMetadataMaxIdleMs());
        }


        List<String> metricReporters = producerProperties.getMetricReporters();
        if (metricReporters != null && !metricReporters.isEmpty()) {
            properties.put(METRIC_REPORTERS, String.join(",", metricReporters));
        }

        if (producerProperties.getMetricsNumSamples() > 0) {
            properties.put(METRICS_NUM_SAMPLES, producerProperties.getMetricsNumSamples());
        }

        if (producerProperties.getMetricsRecordingLevel() != null) {
            properties.put(METRICS_RECORDING_LEVEL, producerProperties.getMetricsRecordingLevel());
        }

        if (producerProperties.getMetricsSampleWindowMs() > 0) {
            properties.put(METRICS_SAMPLE_WINDOW_MS, producerProperties.getMetricsSampleWindowMs());
        }

        properties.put(PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE, producerProperties.isPartitionerAdaptivePartitioningEnable());

        if (producerProperties.getPartitionerAvailabilityTimeoutMs() > 0) {
            properties.put(PARTITIONER_AVAILABILITY_TIMEOUT_MS, producerProperties.getPartitionerAvailabilityTimeoutMs());
        }

        if (producerProperties.getReconnectBackoffMaxMs() > 0) {
            properties.put(RECONNECT_BACKOFF_MAX_MS, producerProperties.getReconnectBackoffMaxMs());
        }

        if (producerProperties.getReconnectBackoffMs() > 0) {
            properties.put(RECONNECT_BACKOFF_MS, producerProperties.getReconnectBackoffMs());
        }

        if (producerProperties.getRetryBackoffMs() > 0) {
            properties.put(RETRY_BACKOFF_MS, producerProperties.getRetryBackoffMs());
        }

        LOG.info("Producer properties");
        properties.entrySet().forEach(prop -> {
            LOG.info("property " + prop.getKey() + " value" + prop.getValue());
        });

        LOG.info("Producer properties ends");
    }

    public static Properties getPropertiesForAdmintClient(final KafkaSinkConfig kafkaSinkConfig) {
        Properties properties = new Properties();
        setCommonServerProperties(properties, kafkaSinkConfig);
        setAuthProperties(properties, kafkaSinkConfig, LOG);
        properties.put(TopicConfig.RETENTION_MS_CONFIG,kafkaSinkConfig.getTopic().getRetentionPeriod());
        return properties;
    }


}
