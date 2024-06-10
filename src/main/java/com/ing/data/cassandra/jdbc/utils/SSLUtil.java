package com.ing.data.cassandra.jdbc.utils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;

public class SSLUtil {

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    public static SSLContext getTrustEverybodySSLContext(String clientCertificateKeyStoreUrl, String clientCertificateKeyStoreType, String clientCertificateKeyStorePassword) throws SQLException {
        KeyManagerFactory kmf;
        KeyManager[] kms = null;

        try {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        catch (NoSuchAlgorithmException nsae) {
            throw new SQLException("Default algorithm definitions for TrustManager and/or KeyManager are invalid.  Check java security properties file.", nsae);
        }

        if (!isNullOrEmpty(clientCertificateKeyStoreUrl)) {
            InputStream ksIS = null;
            try {
                if (!isNullOrEmpty(clientCertificateKeyStoreType)) {
                    KeyStore clientKeyStore = KeyStore.getInstance(clientCertificateKeyStoreType);
                    URL ksURL = new URL(clientCertificateKeyStoreUrl);
                    char[] password = (clientCertificateKeyStorePassword == null) ? new char[0] : clientCertificateKeyStorePassword.toCharArray();
                    ksIS = ksURL.openStream();
                    clientKeyStore.load(ksIS, password);
                    kmf.init(clientKeyStore, password);
                    kms = kmf.getKeyManagers();
                }
            }
            catch (UnrecoverableKeyException uke) {
                throw new SQLException("Could not recover keys from client keystore.  Check password?", uke);
            }
            catch (NoSuchAlgorithmException nsae) {
                throw new SQLException("Unsupported keystore algorithm [" + nsae.getMessage() + "]", nsae);
            }
            catch (KeyStoreException kse) {
                throw new SQLException("Could not create KeyStore instance [" + kse.getMessage() + "]", kse);
            }
            catch (CertificateException nsae) {
                throw new SQLException("Could not load client" + clientCertificateKeyStoreType + " keystore from " + clientCertificateKeyStoreUrl, nsae);
            }
            catch (MalformedURLException mue) {
                throw new SQLException(clientCertificateKeyStoreUrl + " does not appear to be a valid URL.", mue);
            }
            catch (IOException ioe) {
                throw new SQLException("Cannot open " + clientCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]", ioe);
            }
            finally {
                if (ksIS != null) {
                    try {
                        ksIS.close();
                    }
                    catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized so we shouldn't throw this exception
                    }
                }
            }
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kms, new TrustManager[]{new MyTrustEverybodyManager()}, null);
            return sslContext;

        }
        catch (NoSuchAlgorithmException nsae) {
            throw new SQLException("TLS is not a valid SSL protocol.", nsae);
        }
        catch (KeyManagementException kme) {
            throw new SQLException("KeyManagementException: " + kme.getMessage(), kme);
        }
    }

    private static class MyTrustEverybodyManager implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        }

        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
