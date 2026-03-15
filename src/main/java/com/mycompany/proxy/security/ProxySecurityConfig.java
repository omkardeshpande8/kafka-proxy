package com.mycompany.proxy.security;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Properties;

public class ProxySecurityConfig {
    private final SslContext frontendSslContext;
    private final SslContext backendSslContext;

    public ProxySecurityConfig(SslContext frontendSslContext, SslContext backendSslContext) {
        this.frontendSslContext = frontendSslContext;
        this.backendSslContext = backendSslContext;
    }

    public SslContext frontendSslContext() {
        return frontendSslContext;
    }

    public SslContext backendSslContext() {
        return backendSslContext;
    }

    public static ProxySecurityConfig load(Properties props) {
        try {
            SslContext frontend = buildFrontendContext(props);
            SslContext backend = buildBackendContext(props);
            return new ProxySecurityConfig(frontend, backend);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize TLS security contexts", e);
        }
    }

    private static SslContext buildFrontendContext(Properties props) throws Exception {
        if (!Boolean.parseBoolean(props.getProperty("security.frontend.tls.enabled", "false"))) {
            return null;
        }

        String keyStorePath = props.getProperty("security.frontend.tls.keystore.path");
        String keyStorePassword = props.getProperty("security.frontend.tls.keystore.password", "");
        String keyStoreType = props.getProperty("security.frontend.tls.keystore.type", "PKCS12");

        if (keyStorePath == null || keyStorePath.trim().isEmpty()) {
            throw new IllegalArgumentException("security.frontend.tls.keystore.path is required when frontend TLS is enabled");
        }

        KeyManagerFactory kmf = buildKeyManagerFactory(keyStorePath, keyStorePassword, keyStoreType);
        SslContextBuilder builder = SslContextBuilder.forServer(kmf);

        boolean mtlsRequired = Boolean.parseBoolean(props.getProperty("security.frontend.tls.mtls.required", "false"));
        String trustStorePath = props.getProperty("security.frontend.tls.truststore.path");
        String trustStorePassword = props.getProperty("security.frontend.tls.truststore.password", "");
        String trustStoreType = props.getProperty("security.frontend.tls.truststore.type", "PKCS12");

        if (mtlsRequired) {
            if (trustStorePath == null || trustStorePath.trim().isEmpty()) {
                throw new IllegalArgumentException("security.frontend.tls.truststore.path is required when mTLS is enabled");
            }
            TrustManagerFactory tmf = buildTrustManagerFactory(trustStorePath, trustStorePassword, trustStoreType);
            builder.trustManager(tmf);
            builder.clientAuth(ClientAuth.REQUIRE);
        }

        return builder.build();
    }

    private static SslContext buildBackendContext(Properties props) throws Exception {
        if (!Boolean.parseBoolean(props.getProperty("security.backend.tls.enabled", "false"))) {
            return null;
        }

        SslContextBuilder builder = SslContextBuilder.forClient();

        String trustStorePath = props.getProperty("security.backend.tls.truststore.path");
        String trustStorePassword = props.getProperty("security.backend.tls.truststore.password", "");
        String trustStoreType = props.getProperty("security.backend.tls.truststore.type", "PKCS12");
        if (trustStorePath != null && !trustStorePath.trim().isEmpty()) {
            builder.trustManager(buildTrustManagerFactory(trustStorePath, trustStorePassword, trustStoreType));
        }

        boolean clientAuthEnabled = Boolean.parseBoolean(props.getProperty("security.backend.tls.client_auth.enabled", "false"));
        if (clientAuthEnabled) {
            String keyStorePath = props.getProperty("security.backend.tls.keystore.path");
            String keyStorePassword = props.getProperty("security.backend.tls.keystore.password", "");
            String keyStoreType = props.getProperty("security.backend.tls.keystore.type", "PKCS12");
            if (keyStorePath == null || keyStorePath.trim().isEmpty()) {
                throw new IllegalArgumentException("security.backend.tls.keystore.path is required when backend client auth is enabled");
            }
            builder.keyManager(buildKeyManagerFactory(keyStorePath, keyStorePassword, keyStoreType));
        }

        return builder.build();
    }

    private static KeyManagerFactory buildKeyManagerFactory(String keyStorePath, String password, String keyStoreType) throws Exception {
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        try (FileInputStream in = new FileInputStream(keyStorePath)) {
            ks.load(in, password.toCharArray());
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password.toCharArray());
        return kmf;
    }

    private static TrustManagerFactory buildTrustManagerFactory(String trustStorePath, String password, String trustStoreType) throws Exception {
        KeyStore ts = KeyStore.getInstance(trustStoreType);
        try (FileInputStream in = new FileInputStream(trustStorePath)) {
            ts.load(in, password.toCharArray());
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf;
    }
}
