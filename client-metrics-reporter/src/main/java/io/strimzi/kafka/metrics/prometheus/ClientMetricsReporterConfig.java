/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.strimzi.kafka.metrics.prometheus.http.HttpServers;
import io.strimzi.kafka.metrics.prometheus.http.Listener;
import io.strimzi.kafka.metrics.prometheus.otlp.OtlpExporters;
import io.strimzi.kafka.metrics.prometheus.otlp.OtlpSettings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
* Configuration for {@link ClientMetricsReporter}.
*/
public class ClientMetricsReporterConfig extends AbstractConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricsReporterConfig.class);
    private static final String CONFIG_PREFIX = "prometheus.metrics.reporter.";

    /**
     * Configuration key for the listener to expose the metrics.
     */
    public static final String LISTENER_CONFIG = CONFIG_PREFIX + "listener";

    /**
     * Default value for the listener configuration.
     */
    public static final String LISTENER_CONFIG_DEFAULT = "http://:8080";
    private static final String LISTENER_CONFIG_DOC = "The HTTP listener to expose the metrics.";

    /**
     * Configuration key to determine if the listener is enabled or not.
     */
    public static final String LISTENER_ENABLE_CONFIG = CONFIG_PREFIX + "listener.enable";

    /**
     * Default value for the listener enabled configuration.
     */
    public static final Boolean LISTENER_ENABLE_CONFIG_DEFAULT = true;
    private static final String LISTENER_ENABLE_CONFIG_DOC = "Enable the listener to expose the metrics.";

    /**
     * Configuration key for the allowlist of metrics to collect.
     */
    public static final String ALLOWLIST_CONFIG = CONFIG_PREFIX + "allowlist";

    /**
     * Default value for the allowlist configuration.
     */
    public static final String ALLOWLIST_CONFIG_DEFAULT = ".*";
    private static final String ALLOWLIST_CONFIG_DOC = "A comma separated list of regex patterns to specify the metrics to collect.";

    /**
     * Configuration key to determine if the metrics are pushed to an OTLP endpoint or not.
     */
    public static final String OTLP_ENABLE_CONFIG = CONFIG_PREFIX + "otlp.enable";

    /**
     * Default value for the OTLP enabled configuration.
     */
    public static final Boolean OTLP_ENABLE_CONFIG_DEFAULT = false;
    private static final String OTLP_ENABLE_CONFIG_DOC = "Enable pushing the metrics to an OTLP endpoint.";

    /**
     * Configuration key for the OTLP endpoint to push the metrics to.
     */
    public static final String OTLP_ENDPOINT_CONFIG = CONFIG_PREFIX + "otlp.endpoint";
    private static final String OTLP_ENDPOINT_CONFIG_DOC = "The OTLP endpoint to push the metrics to, in the http://[host]:[port] " +
            "or https://[host]:[port] format. When not set, the standard OpenTelemetry configuration applies, for example the " +
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT environment variables.";

    /**
     * Configuration key for the OTLP transport protocol.
     */
    public static final String OTLP_PROTOCOL_CONFIG = CONFIG_PREFIX + "otlp.protocol";
    private static final String OTLP_PROTOCOL_CONFIG_DOC = "The OTLP transport protocol, either grpc or http/protobuf. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to grpc.";

    /**
     * Configuration key for the interval between pushes to the OTLP endpoint.
     */
    public static final String OTLP_INTERVAL_SECONDS_CONFIG = CONFIG_PREFIX + "otlp.interval.seconds";
    private static final String OTLP_INTERVAL_SECONDS_CONFIG_DOC = "The interval in seconds between pushes to the OTLP endpoint. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to 60 seconds.";

    /**
     * Configuration key for the resource attributes added to the metrics pushed to the OTLP endpoint.
     */
    public static final String OTLP_RESOURCE_ATTRIBUTES_CONFIG = CONFIG_PREFIX + "otlp.resource.attributes";
    private static final String OTLP_RESOURCE_ATTRIBUTES_CONFIG_DOC = "A comma separated list of key=value pairs to add as " +
            "resource attributes to the metrics pushed to the OTLP endpoint, in the format of the OTEL_RESOURCE_ATTRIBUTES " +
            "environment variable. Keys and values can be percent-encoded.";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(LISTENER_CONFIG, ConfigDef.Type.STRING, LISTENER_CONFIG_DEFAULT, new Listener.ListenerValidator(), ConfigDef.Importance.HIGH, LISTENER_CONFIG_DOC)
            .define(ALLOWLIST_CONFIG, ConfigDef.Type.LIST, ALLOWLIST_CONFIG_DEFAULT, ConfigDef.Importance.HIGH, ALLOWLIST_CONFIG_DOC)
            .define(LISTENER_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, LISTENER_ENABLE_CONFIG_DEFAULT, ConfigDef.Importance.HIGH, LISTENER_ENABLE_CONFIG_DOC)
            .define(OTLP_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, OTLP_ENABLE_CONFIG_DEFAULT, ConfigDef.Importance.HIGH, OTLP_ENABLE_CONFIG_DOC)
            .define(OTLP_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.LambdaValidator.with(
                    (name, value) -> {
                        if (value != null && !((String) value).startsWith("http://") && !((String) value).startsWith("https://")) {
                            throw new ConfigException(name, value, "Endpoint must be of format http://[host]:[port] or https://[host]:[port]");
                        }
                    },
                    () -> "http://[host]:[port] or https://[host]:[port]"), ConfigDef.Importance.HIGH, OTLP_ENDPOINT_CONFIG_DOC)
            .define(OTLP_PROTOCOL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.LambdaValidator.with(
                    (name, value) -> {
                        if (value != null && !"grpc".equals(value) && !"http/protobuf".equals(value)) {
                            throw new ConfigException(name, value, "Protocol must be grpc or http/protobuf");
                        }
                    },
                    () -> "[grpc, http/protobuf]"), ConfigDef.Importance.MEDIUM, OTLP_PROTOCOL_CONFIG_DOC)
            .define(OTLP_INTERVAL_SECONDS_CONFIG, ConfigDef.Type.INT, null, ConfigDef.LambdaValidator.with(
                    (name, value) -> {
                        if (value != null && (Integer) value < 1) {
                            throw new ConfigException(name, value, "Interval must be at least 1");
                        }
                    },
                    () -> "[1,...]"), ConfigDef.Importance.MEDIUM, OTLP_INTERVAL_SECONDS_CONFIG_DOC)
            .define(OTLP_RESOURCE_ATTRIBUTES_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.LambdaValidator.with(
                    (name, value) -> {
                        @SuppressWarnings("unchecked")
                        List<String> entries = (List<String>) value;
                        OtlpSettings.parseResourceAttributes(name, entries);
                    },
                    () -> "key1=value1,key2=value2"), ConfigDef.Importance.MEDIUM, OTLP_RESOURCE_ATTRIBUTES_CONFIG_DOC);

    final Listener listener;
    final boolean listenerEnabled;
    final PrometheusRegistry registry;
    final Pattern allowlist;
    final boolean otlpEnabled;
    final OtlpSettings otlpSettings;

    /**
     * Constructor.
     *
     * @param props the configuration properties.
     * @param registry the metrics registry
     */
    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    public ClientMetricsReporterConfig(Map<?, ?> props, PrometheusRegistry registry) {
        super(CONFIG_DEF, props);
        this.listener = Listener.parseListener(getString(LISTENER_CONFIG));
        this.allowlist = compileAllowlist(getList(ALLOWLIST_CONFIG));
        this.listenerEnabled = getBoolean(LISTENER_ENABLE_CONFIG);
        this.registry = registry;
        this.otlpEnabled = getBoolean(OTLP_ENABLE_CONFIG);
        this.otlpSettings = new OtlpSettings(
                getString(OTLP_ENDPOINT_CONFIG),
                getString(OTLP_PROTOCOL_CONFIG),
                getInt(OTLP_INTERVAL_SECONDS_CONFIG),
                OtlpSettings.parseResourceAttributes(OTLP_RESOURCE_ATTRIBUTES_CONFIG, getList(OTLP_RESOURCE_ATTRIBUTES_CONFIG)));
    }

    /**
     * Check if a metric is allowed.
     *
     * @param name the name of the metric.
     * @return true if the metric is allowed, false otherwise.
     */
    public boolean isAllowed(String name) {
        return allowlist.matcher(name).matches();
    }

    /**
     * The configured allowlist.
     * @return The Pattern for the allowlist
     */
    public Pattern allowlist() {
        return allowlist;
    }

    Pattern compileAllowlist(List<String> allowlist) {
        for (String entry : allowlist) {
            try {
                Pattern.compile(entry);
            } catch (PatternSyntaxException pse) {
                throw new ConfigException("Invalid regex pattern found in " + ALLOWLIST_CONFIG + ": " + entry);
            }
        }
        String joined = allowlist.stream().map(v -> "(" + v + ")").collect(Collectors.joining("|"));
        return Pattern.compile(joined);
    }

    /**
     * Gets the listener.
     *
     * @return the listener.
     */
    public String listener() {
        return listener.toString();
    }

    /**
     * Check whether the listener is enabled.
     *
     * @return {@code true} if the listener is enabled, {@code false} otherwise.
     */
    public boolean isListenerEnabled() {
        return listenerEnabled;
    }

    /**
     * Check whether pushing the metrics to an OTLP endpoint is enabled.
     *
     * @return {@code true} if pushing to an OTLP endpoint is enabled, {@code false} otherwise.
     */
    public boolean isOtlpEnabled() {
        return otlpEnabled;
    }

    /**
     * Gets the OTLP exporter settings.
     *
     * @return the OTLP exporter settings.
     */
    public OtlpSettings otlpSettings() {
        return otlpSettings;
    }

    @Override
    public String toString() {
        return "ClientMetricsReporterConfig{" +
                "listener=" + listener +
                ", listenerEnabled=" + listenerEnabled +
                ", allowlist=" + allowlist +
                ", otlpEnabled=" + otlpEnabled +
                ", otlpSettings=" + otlpSettings +
                '}';
    }

    /**
     * Start the HTTP server for exposing metrics.
     *
     * @return An optional ServerCounter instance if {@link #LISTENER_ENABLE_CONFIG} is enabled, otherwise empty.
     */
    public synchronized Optional<HttpServers.ServerCounter> startHttpServer() {
        if (!listenerEnabled) {
            LOG.info("HTTP server listener not enabled");
            return Optional.empty();
        }
        HttpServers.ServerCounter server = HttpServers.getOrCreate(listener, registry);
        LOG.info("HTTP server listening on http://{}:{}", listener.host, server.port());
        return Optional.of(server);
    }

    /**
     * Start the OTLP exporter for pushing metrics.
     *
     * @return An optional ExporterCounter instance if {@link #OTLP_ENABLE_CONFIG} is enabled, otherwise empty.
     */
    public synchronized Optional<OtlpExporters.ExporterCounter> startOtlpExporter() {
        if (!otlpEnabled) {
            return Optional.empty();
        }
        OtlpExporters.checkCanStart();
        OtlpExporters.ExporterCounter exporter = OtlpExporters.getOrCreate(otlpSettings, registry);
        LOG.info("Pushing metrics to an OTLP endpoint with {}", otlpSettings);
        return Optional.of(exporter);
    }
}
