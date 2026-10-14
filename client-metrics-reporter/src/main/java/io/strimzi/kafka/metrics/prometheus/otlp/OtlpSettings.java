/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import org.apache.kafka.common.config.ConfigException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The settings of an OTLP exporter. Components configured with equal settings share a single exporter, so the
 * metrics of a JVM are pushed once rather than once per component.
 * This class does not depend on the OpenTelemetry exporter, so it can be used when the exporter is not on the classpath.
 */
public final class OtlpSettings {
    private final String endpoint;
    private final String protocol;
    private final Integer intervalSeconds;
    private final Map<String, String> resourceAttributes;
    private final OtlpTls tls;

    /**
     * Constructor, without TLS settings.
     *
     * @param endpoint The OTLP endpoint, or null to use the OpenTelemetry configuration
     * @param protocol The OTLP transport protocol, or null to use the OpenTelemetry configuration
     * @param intervalSeconds The interval between pushes, or null to use the OpenTelemetry configuration
     * @param resourceAttributes The resource attributes to add to the exported metrics
     */
    public OtlpSettings(String endpoint, String protocol, Integer intervalSeconds, Map<String, String> resourceAttributes) {
        this(endpoint, protocol, intervalSeconds, resourceAttributes, OtlpTls.NONE);
    }

    /**
     * Constructor.
     *
     * @param endpoint The OTLP endpoint, or null to use the OpenTelemetry configuration
     * @param protocol The OTLP transport protocol, or null to use the OpenTelemetry configuration
     * @param intervalSeconds The interval between pushes, or null to use the OpenTelemetry configuration
     * @param resourceAttributes The resource attributes to add to the exported metrics
     * @param tls The TLS settings
     */
    public OtlpSettings(String endpoint, String protocol, Integer intervalSeconds, Map<String, String> resourceAttributes, OtlpTls tls) {
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.intervalSeconds = intervalSeconds;
        this.resourceAttributes = Map.copyOf(resourceAttributes);
        this.tls = Objects.requireNonNull(tls);
    }

    /**
     * Parse resource attributes in the format of the OTEL_RESOURCE_ATTRIBUTES environment variable: a list of
     * key=value pairs, where keys and values can be percent-encoded.
     *
     * @param name The name of the configuration, used in error messages
     * @param entries The key=value pairs
     * @return The resource attributes
     */
    public static Map<String, String> parseResourceAttributes(String name, List<String> entries) {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (String entry : entries) {
            int eqIdx = entry.indexOf('=');
            if (eqIdx < 1) {
                throw new ConfigException(name, entries, "Resource attributes must be of format key=value");
            }
            attributes.put(decode(entry.substring(0, eqIdx).trim()), decode(entry.substring(eqIdx + 1).trim()));
        }
        return attributes;
    }

    private static String decode(String value) {
        // URLDecoder also decodes '+' into a space, which percent-encoding does not
        return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    /**
     * The OTLP endpoint.
     * @return The endpoint, or null to use the OpenTelemetry configuration
     */
    public String endpoint() {
        return endpoint;
    }

    /**
     * The OTLP transport protocol.
     * @return The protocol, or null to use the OpenTelemetry configuration
     */
    public String protocol() {
        return protocol;
    }

    /**
     * The interval between pushes.
     * @return The interval in seconds, or null to use the OpenTelemetry configuration
     */
    public Integer intervalSeconds() {
        return intervalSeconds;
    }

    /**
     * The resource attributes to add to the exported metrics.
     * @return The resource attributes
     */
    public Map<String, String> resourceAttributes() {
        return resourceAttributes;
    }

    /**
     * The TLS settings.
     * @return The TLS settings
     */
    public OtlpTls tls() {
        return tls;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OtlpSettings)) return false;
        OtlpSettings other = (OtlpSettings) o;
        return Objects.equals(endpoint, other.endpoint)
                && Objects.equals(protocol, other.protocol)
                && Objects.equals(intervalSeconds, other.intervalSeconds)
                && resourceAttributes.equals(other.resourceAttributes)
                && tls.equals(other.tls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, protocol, intervalSeconds, resourceAttributes, tls);
    }

    @Override
    public String toString() {
        return "OtlpSettings{" +
                "endpoint=" + endpoint +
                ", protocol=" + protocol +
                ", intervalSeconds=" + intervalSeconds +
                ", resourceAttributes=" + resourceAttributes +
                ", tls=" + tls +
                '}';
    }
}
