/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.strimzi.kafka.metrics.prometheus.http.HttpServers;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.net.BindException;
import java.util.Map;
import java.util.Optional;

import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.ALLOWLIST_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.LISTENER_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.LISTENER_ENABLE_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.OTLP_ENABLE_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.OTLP_ENDPOINT_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.OTLP_INTERVAL_SECONDS_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.OTLP_PROTOCOL_CONFIG;
import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ClientMetricsReporterConfigTest {

    @Test
    public void testDefaults() {
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(Map.of(), new PrometheusRegistry());
        assertEquals(ClientMetricsReporterConfig.LISTENER_CONFIG_DEFAULT, config.listener());
        assertTrue(config.isAllowed("random_name"));
    }

    @Test
    public void testOverrides() {
        Map<String, String> props = Map.of(
            LISTENER_CONFIG, "http://:0",
            ALLOWLIST_CONFIG, "kafka_server.*");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());

        assertEquals("http://:0", config.listener());
        assertFalse(config.isAllowed("random_name"));
        assertTrue(config.isAllowed("kafka_server_metric"));
    }

    @Test
    public void testAllowList() {
        Map<String, String> props = Map.of(ALLOWLIST_CONFIG, "kafka_server.*,kafka_network.*");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());

        assertTrue(config.allowlist().pattern().contains("kafka_server.*"));
        assertTrue(config.allowlist().pattern().contains("kafka_network.*"));

        assertFalse(config.isAllowed("random_name"));
        assertTrue(config.isAllowed("kafka_server_metric"));
        assertTrue(config.isAllowed("kafka_network_metric"));

        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(ALLOWLIST_CONFIG, "hell[o,s]world"), null));
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(ALLOWLIST_CONFIG, "hello\\,world"), null));
    }

    @Test
    public void testIsListenerEnabled() {
        Map<String, String> props = Map.of(
            LISTENER_ENABLE_CONFIG, "true",
            LISTENER_CONFIG, "http://:0");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());
        Optional<HttpServers.ServerCounter> httpServerOptional = config.startHttpServer();

        assertTrue(config.isListenerEnabled());
        assertTrue(httpServerOptional.isPresent());
        HttpServers.release(httpServerOptional.get());
    }

    @Test
    public void testIsListenerDisabled() {
        Map<String, Boolean> props = Map.of(LISTENER_ENABLE_CONFIG, false);
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());
        Optional<HttpServers.ServerCounter> httpServerOptional = config.startHttpServer();

        assertTrue(httpServerOptional.isEmpty());
        assertFalse(config.isListenerEnabled());
    }

    @Test
    public void testStartHttpServer() {
        Map<String, String> props = Map.of(LISTENER_CONFIG, "http://:0");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());
        Optional<HttpServers.ServerCounter> httpServerOptional = config.startHttpServer();
        assertTrue(httpServerOptional.isPresent());

        ClientMetricsReporterConfig config2 = new ClientMetricsReporterConfig(props, new PrometheusRegistry());
        Optional<HttpServers.ServerCounter> httpServerOptional2 = config2.startHttpServer();
        assertTrue(httpServerOptional2.isPresent());

        props = Map.of(LISTENER_CONFIG, "http://:" + httpServerOptional.get().port());
        ClientMetricsReporterConfig config3 = new ClientMetricsReporterConfig(props, new PrometheusRegistry());
        Exception exc = assertThrows(RuntimeException.class, config3::startHttpServer);
        assertInstanceOf(BindException.class, exc.getCause());

        HttpServers.release(httpServerOptional.get());
        HttpServers.release(httpServerOptional2.get());
    }

    @Test
    public void testOtlpDefaults() {
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(Map.of(), new PrometheusRegistry());

        assertFalse(config.isOtlpEnabled());
        assertNull(config.otlpSettings().endpoint());
        assertNull(config.otlpSettings().protocol());
        assertNull(config.otlpSettings().intervalSeconds());
        assertTrue(config.otlpSettings().resourceAttributes().isEmpty());
        assertTrue(config.startOtlpExporter().isEmpty());
    }

    @Test
    public void testOtlpOverrides() {
        Map<String, String> props = Map.of(
            OTLP_ENABLE_CONFIG, "true",
            OTLP_ENDPOINT_CONFIG, "http://my-collector:4318",
            OTLP_PROTOCOL_CONFIG, "http/protobuf",
            OTLP_INTERVAL_SECONDS_CONFIG, "15",
            OTLP_RESOURCE_ATTRIBUTES_CONFIG, "k8s.pod.name=my-pod, k8s.namespace.name=my-namespace");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());

        assertTrue(config.isOtlpEnabled());
        assertEquals("http://my-collector:4318", config.otlpSettings().endpoint());
        assertEquals("http/protobuf", config.otlpSettings().protocol());
        assertEquals(Integer.valueOf(15), config.otlpSettings().intervalSeconds());
        assertEquals(Map.of("k8s.pod.name", "my-pod", "k8s.namespace.name", "my-namespace"),
                config.otlpSettings().resourceAttributes());
    }

    @Test
    public void testOtlpValidation() {
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(OTLP_ENDPOINT_CONFIG, "my-collector:4317"), null));
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(OTLP_PROTOCOL_CONFIG, "http/json"), null));
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(OTLP_INTERVAL_SECONDS_CONFIG, "0"), null));
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(OTLP_RESOURCE_ATTRIBUTES_CONFIG, "k8s.pod.name"), null));
        assertThrows(ConfigException.class,
                () -> new ClientMetricsReporterConfig(Map.of(OTLP_RESOURCE_ATTRIBUTES_CONFIG, "=my-pod"), null));
    }

    @Test
    public void testOtlpResourceAttributeValueMayContainEquals() {
        Map<String, String> props = Map.of(OTLP_RESOURCE_ATTRIBUTES_CONFIG, "key=a=b");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());

        assertEquals(Map.of("key", "a=b"), config.otlpSettings().resourceAttributes());
    }

    @Test
    public void testOtlpResourceAttributesArePercentDecoded() {
        Map<String, String> props = Map.of(OTLP_RESOURCE_ATTRIBUTES_CONFIG, "team=a%2Cb%3Dc,owner=Jane%20Doe,version=1+2");
        ClientMetricsReporterConfig config = new ClientMetricsReporterConfig(props, new PrometheusRegistry());

        assertEquals(Map.of("team", "a,b=c", "owner", "Jane Doe", "version", "1+2"), config.otlpSettings().resourceAttributes());
    }
}
