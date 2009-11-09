package org.nordugrid.is.query.tasks;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import netscape.ldap.LDAPAttribute;
import netscape.ldap.LDAPAttributeSet;
import netscape.ldap.LDAPConnection;
import netscape.ldap.LDAPEntry;
import netscape.ldap.LDAPException;
import netscape.ldap.LDAPSearchResults;

import org.apache.log4j.Logger;
import org.nordugrid.model.ARCResource;
import org.nordugrid.multithread.TaskFailedException;
import org.nordugrid.utils.LDAPUtilities;

public final class LDAPQueryAuthorizedQueuesTask extends QueryTask {
  
  private int ldapTimeOutSeconds = 5;
  private int ldapConnectMillis = 5000;

	private static final Logger log = Logger
			.getLogger(LDAPQueryAuthorizedQueuesTask.class);

	public LDAPQueryAuthorizedQueuesTask(String clusterURL, String dn, long _ldapTimeOutMillis) {
		super(clusterURL, dn);
    ldapConnectMillis = (int)_ldapTimeOutMillis;
    ldapTimeOutSeconds = (int) _ldapTimeOutMillis/1000;
	}

	/**
	 * Query resource using Netscape Directory SDK.
	 * 
	 * @throws TaskFailedException
	 */
	public void process() throws TaskFailedException {
		Collection foundResources = new HashSet();
		LDAPConnection conn = null;
		try {
			URI ln = new URI(this.getResourceURL());

			String filter = "(|(objectclass=nordugrid-cluster)(&(objectclass=nordugrid-queue)(nordugrid-queue-status=active)"
					+ ")(&(objectclass=nordugrid-authuser)(nordugrid-authuser-sn="
					+ this.getDn() + ")))";
			log.debug("Search filter: " + filter);
      System.out.println("Search filter: " + filter);
			log.debug("Host: " + ln.getHost());
			log.debug("Bind DN: " + ln.getPath());
			log.debug("Port: " + ln.getPort());
      System.out.println("Searching LDAP");
			conn = new LDAPConnection();
			LDAPSearchResults results = LDAPUtilities.runLDAPSearch(conn, ln
					.getHost(), ln.getPort(), ln.getPath().substring(1),
					filter, null, ldapConnectMillis, ldapTimeOutSeconds);
      System.out.println("Done searching LDAP");
			LDAPAttributeSet cluster = null;
			LDAPAttributeSet queue = null;
			LDAPAttributeSet authuser = null;
			for (; results.hasMoreElements();) {
				try {
					LDAPEntry result = results.next();
					// System.err.println("RESULT: "+result.toString());
					LDAPAttributeSet attrs = result.getAttributeSet();

					String oclass = null;
					for (int i = 0; i < attrs.size(); i++) {
						oclass = (String) (attrs.getAttribute("objectClass")
								.getStringValueArray()[i]);
						if (oclass.equals("nordugrid-cluster")) {
							cluster = attrs;
							break;
						} else if (oclass.equals("nordugrid-queue")) {
							queue = attrs;
							break;
						} else if (oclass.equals("nordugrid-authuser")) {
							authuser = attrs;
							long start = System.currentTimeMillis();
							ARCResource r = new ARCResource(
									DirToJNDIConverter(cluster),
									DirToJNDIConverter(queue),
									DirToJNDIConverter(authuser));
							long end = System.currentTimeMillis();
							foundResources.add(r);
							break;
						}
					}
				} catch (Exception e) {
					// e.printStackTrace();
					continue;
				}
			}
			if (conn != null)
				conn.disconnect();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (LDAPException e) {
			// in case we have not succeded in querying information, throw a
			// special
			// exception
			throw new TaskFailedException(e.getMessage());
		} finally {
			if (conn != null && conn.isConnected())
				try {
					conn.disconnect();
				} catch (LDAPException e) {
					e.printStackTrace();
					throw new TaskFailedException(e.getMessage());
				}
		}
		setResult(foundResources);
	}

	private Attributes DirToJNDIConverter(LDAPAttributeSet directory) {
		Attributes resulting = new BasicAttributes();
		Enumeration en = directory.getAttributes();
		while (en.hasMoreElements()) {
			LDAPAttribute current = (LDAPAttribute) en.nextElement();
			// Fix by Frederik Orellana
			Attribute temp = null;
			Set set = new HashSet();
			for (int i = 0; i < current.getStringValueArray().length; ++i) {
				set.add(current.getStringValueArray()[i]);
			}
			temp = new BasicAttribute(current.getName(), set);
			resulting.put(temp);
		}
		return resulting;
	}

	public String toString() {
		return "Ldap query work: " + this.getResourceURL() + ", "
				+ this.getDn();
	}
}
