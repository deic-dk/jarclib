package org.nordugrid.utils;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.cert.X509Certificate;
import org.globus.gsi.CertUtil;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;

/**
 * Useful methods for working with credentials.
 * 
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class CredentialsUtilities {
	
	public static GlobusCredential createProxy(String useKeyFilename, String userCertFilename, String password, int lifetime, int strength)
			throws IOException, GeneralSecurityException {
		OpenSSLKey key;

		key = new BouncyCastleOpenSSLKey(useKeyFilename);
	
		// getting user certificate
		X509Certificate userCert = CertUtil.loadCertificate(userCertFilename);
		return createProxy(key, userCert, password, lifetime, strength);

	}

	public static GlobusCredential createProxy(OpenSSLKey key, X509Certificate userCert, String password, int lifetime, int strength)
			throws InvalidKeyException, GeneralSecurityException {

		// decrypting the password
		if (key.isEncrypted()) {
			key.decrypt(password);
		}

		// Type of the proxy. Hardcoded, as it's the only thing we'll use.
		int proxyType = GSIConstants.DELEGATION_FULL;

		// factory for proxy generation
		BouncyCastleCertProcessingFactory factory = BouncyCastleCertProcessingFactory.getDefault();

		GlobusCredential myCredentials = factory.createCredential(new X509Certificate[] { userCert }, key.getPrivateKey(), strength, lifetime,
				proxyType, (X509ExtensionSet) null);
		return myCredentials;
	}
}
