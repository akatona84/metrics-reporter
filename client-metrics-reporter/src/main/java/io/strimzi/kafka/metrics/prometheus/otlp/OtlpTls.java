/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The TLS settings of an OTLP exporter, in PEM format. The names of the configurations follow the Kafka PEM
 * configurations, and are defined without prefix, so that other components, like the HTTP Bridge, can use them with
 * their own prefix.
 */
public final class OtlpTls {
    /**
     * No TLS settings, so the JVM default trust store is used and no client certificate is sent.
     */
    public static final OtlpTls NONE = new OtlpTls(null, null, null);

    /**
     * Configuration for the path to a PEM file with the trusted certificates.
     */
    public static final String TRUSTSTORE_LOCATION = "otlp.ssl.truststore.location";

    /**
     * Configuration for the trusted certificates in PEM format.
     */
    public static final String TRUSTSTORE_CERTIFICATES = "otlp.ssl.truststore.certificates";

    /**
     * Configuration for the path to a PEM file with the private key and the certificate chain.
     */
    public static final String KEYSTORE_LOCATION = "otlp.ssl.keystore.location";

    /**
     * Configuration for the certificate chain in PEM format.
     */
    public static final String KEYSTORE_CERTIFICATE_CHAIN = "otlp.ssl.keystore.certificate.chain";

    /**
     * Configuration for the private key in PEM format.
     */
    public static final String KEYSTORE_KEY = "otlp.ssl.keystore.key";

    private static final Pattern PEM_BLOCK = Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----.*?-----END \\1-----", Pattern.DOTALL);

    private final String trustedCertificates;
    private final String certificateChain;
    private final String privateKey;

    private OtlpTls(String trustedCertificates, String certificateChain, String privateKey) {
        this.trustedCertificates = trustedCertificates;
        this.certificateChain = certificateChain;
        this.privateKey = privateKey;
    }

    /**
     * Create the TLS settings from the configurations. The certificates can be configured either as a path to a PEM
     * file or inline, but not both. Files are read when the settings are created, so that errors are reported when
     * the component is configured.
     *
     * @param prefix The prefix of the configurations, used in error messages
     * @param config The values of the TLS configurations, by name without prefix. Missing or null values are not set.
     * @return The TLS settings
     */
    public static OtlpTls fromConfig(String prefix, Map<String, String> config) {
        validateCombinations(prefix, config);

        String trustedCertificates = trustedCertificates(prefix, config);
        String[] keystore = keystore(prefix, config);
        if (trustedCertificates == null && keystore == null) {
            return NONE;
        }
        return new OtlpTls(trustedCertificates, keystore != null ? keystore[0] : null, keystore != null ? keystore[1] : null);
    }

    private static void validateCombinations(String prefix, Map<String, String> config) {
        if (config.get(TRUSTSTORE_LOCATION) != null && config.get(TRUSTSTORE_CERTIFICATES) != null) {
            throw new ConfigException(prefix + TRUSTSTORE_LOCATION + " and " + prefix + TRUSTSTORE_CERTIFICATES + " cannot be set together");
        }
        if (config.get(KEYSTORE_LOCATION) != null && (config.get(KEYSTORE_CERTIFICATE_CHAIN) != null || config.get(KEYSTORE_KEY) != null)) {
            throw new ConfigException(prefix + KEYSTORE_LOCATION + " cannot be set together with " +
                    prefix + KEYSTORE_CERTIFICATE_CHAIN + " or " + prefix + KEYSTORE_KEY);
        }
        if ((config.get(KEYSTORE_CERTIFICATE_CHAIN) == null) != (config.get(KEYSTORE_KEY) == null)) {
            throw new ConfigException(prefix + KEYSTORE_CERTIFICATE_CHAIN + " and " + prefix + KEYSTORE_KEY + " must be set together");
        }
    }

    private static String trustedCertificates(String prefix, Map<String, String> config) {
        String location = config.get(TRUSTSTORE_LOCATION);
        String trustedCertificates = location != null ? read(prefix + TRUSTSTORE_LOCATION, location) : config.get(TRUSTSTORE_CERTIFICATES);
        if (trustedCertificates != null && pem(trustedCertificates, "CERTIFICATE").isEmpty()) {
            throw new ConfigException("The trusted certificates of " + prefix + "otlp.ssl.truststore do not contain any PEM certificate");
        }
        return trustedCertificates;
    }

    /**
     * @return The certificate chain and the private key, or null when no client certificate is configured
     */
    private static String[] keystore(String prefix, Map<String, String> config) {
        String location = config.get(KEYSTORE_LOCATION);
        String certificateChain = config.get(KEYSTORE_CERTIFICATE_CHAIN);
        String privateKey = config.get(KEYSTORE_KEY);
        if (location != null) {
            // Like the Kafka PEM keystore, the file contains both the private key and the certificate chain
            String keystore = read(prefix + KEYSTORE_LOCATION, location);
            certificateChain = pem(keystore, "CERTIFICATE");
            privateKey = pem(keystore, "PRIVATE KEY");
        }
        if (certificateChain == null) {
            return null;
        }
        if (pem(certificateChain, "CERTIFICATE").isEmpty()) {
            throw new ConfigException("The certificate chain of " + prefix + "otlp.ssl.keystore does not contain any PEM certificate");
        }
        if (pem(privateKey, "PRIVATE KEY").isEmpty()) {
            throw new ConfigException("The private key of " + prefix + "otlp.ssl.keystore does not contain any PEM private key");
        }
        return new String[] {certificateChain, privateKey};
    }

    private static String read(String name, String location) {
        try {
            return Files.readString(Path.of(location), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            throw new ConfigException(name, location, "Cannot read the file: " + e.getMessage());
        }
    }

    /**
     * Extract the PEM blocks whose type ends with the given suffix, for example CERTIFICATE or PRIVATE KEY (which
     * covers the RSA PRIVATE KEY and EC PRIVATE KEY types too).
     */
    /* test */ static String pem(String content, String typeSuffix) {
        StringBuilder blocks = new StringBuilder();
        Matcher matcher = PEM_BLOCK.matcher(content);
        while (matcher.find()) {
            if (matcher.group(1).endsWith(typeSuffix)) {
                blocks.append(matcher.group()).append('\n');
            }
        }
        return blocks.toString();
    }

    /**
     * Whether no TLS settings are configured.
     * @return True when no TLS settings are configured
     */
    public boolean isEmpty() {
        return trustedCertificates == null && certificateChain == null;
    }

    /**
     * The trusted certificates.
     * @return The trusted certificates in PEM format, or null to use the JVM default trust store
     */
    public String trustedCertificates() {
        return trustedCertificates;
    }

    /**
     * The certificate chain for mTLS.
     * @return The certificate chain in PEM format, or null when no client certificate is sent
     */
    public String certificateChain() {
        return certificateChain;
    }

    /**
     * The private key for mTLS.
     * @return The private key in PEM format, or null when no client certificate is sent
     */
    public String privateKey() {
        return privateKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OtlpTls)) return false;
        OtlpTls other = (OtlpTls) o;
        return Objects.equals(trustedCertificates, other.trustedCertificates)
                && Objects.equals(certificateChain, other.certificateChain)
                && Objects.equals(privateKey, other.privateKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustedCertificates, certificateChain, privateKey);
    }

    @Override
    public String toString() {
        // Never print the private key
        return "OtlpTls{" +
                "trustedCertificates=" + (trustedCertificates != null ? "set" : "not set") +
                ", clientCertificate=" + (certificateChain != null ? "set" : "not set") +
                '}';
    }
}
