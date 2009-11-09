package org.nordugrid.utils;

import netscape.ldap.LDAPConnection;
import netscape.ldap.LDAPConstraints;
import netscape.ldap.LDAPException;
import netscape.ldap.LDAPSearchConstraints;
import netscape.ldap.LDAPSearchResults;
import netscape.ldap.LDAPv2;

/**
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 *
 */
public class LDAPUtilities {
	
	public static LDAPSearchResults runLDAPSearch(LDAPConnection ld, String hostname, int port, String baseDN, String filter, 
			String attributes, int connectionTimeoutInMs, int searchTimeoutInS) throws LDAPException {		
		// connection timeout
		ld.setConnectTimeout(connectionTimeoutInMs);
		ld.connect(hostname, port);
		LDAPConstraints ldc = new LDAPConstraints();
		ldc.setTimeLimit(connectionTimeoutInMs);
		ld.authenticate(LDAPConnection.LDAP_VERSION, null, null, ldc);
		LDAPSearchConstraints ldcSearch = new LDAPSearchConstraints();
		ldcSearch.setTimeLimit(searchTimeoutInS * 1000); // should be same as server time limit. But in case server-side is buggy...
		ldcSearch.setServerTimeLimit(searchTimeoutInS);
		ldcSearch.setMaxResults(0); // unlimited	
    System.out.println("Searching LDAP with parameters "+ldcSearch);
		// search is asynchronous function! Therefore one cannot close the connection right after the call to this function.
		LDAPSearchResults result = ld.search(baseDN, LDAPv2.SCOPE_SUB, filter, null, false, ldcSearch);
    System.out.println("Done searching LDAP");

		return result;
	}
}
