package org.nordugrid.is.query.tasks;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
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
import org.nordugrid.model.ARCJob;
import org.nordugrid.multithread.TaskFailedException;
import org.nordugrid.utils.LDAPUtilities;

public final class LDAPQueryUserJobsTask extends QueryTask {

	private static Logger log = Logger.getLogger(LDAPQueryUserJobsTask.class);
  private int ldapTimeOutSeconds = 5;
  private int ldapConnectMillis = 5000;
	
	public LDAPQueryUserJobsTask(String clusterURL, String dn, long _ldapTimeOutMillis ) {
		super(clusterURL, dn);
    ldapConnectMillis = (int)_ldapTimeOutMillis;
    ldapTimeOutSeconds = (int) _ldapTimeOutMillis/1000;
	}

	/**
	 * Query resource using Netscape Directory SDK.
	 * @throws TaskFailedException 
	 */
	public void process() throws TaskFailedException {
		Collection foundResources = new HashSet();				
		LDAPConnection conn = null;
		try {
			URI ln = new URI(this.getResourceURL());
			
			String filter = "(&(objectclass=nordugrid-job)(nordugrid-job-globalowner=" + this.getDn() + "))";
			log.debug("Query target: " + ln.getHost());
			log.debug("Query filter: " + filter);
			conn = new LDAPConnection();
			LDAPSearchResults results = LDAPUtilities.runLDAPSearch(conn, ln.getHost(), ln.getPort(), ln.getPath().substring(1), filter, null, ldapConnectMillis, ldapTimeOutSeconds);

			for (; results.hasMoreElements();) {
				try {
					LDAPEntry result = results.next();
					
					LDAPAttributeSet attrs = result.getAttributeSet();
					String oclass = null;
					for (int i = 0; i < attrs.size(); i++) {
						oclass = (String) (attrs.getAttribute("objectClass").getStringValueArray()[i]);
						if (oclass.equals("nordugrid-job")) {							
							long start = System.currentTimeMillis();
							ARCJob job = new ARCJob(DirToJNDIConverter(attrs));
							long end = System.currentTimeMillis();
							foundResources.add(job);
							break;
						}
					}
				}
				catch (Exception e) {
					
					continue;
				}
			}
			if (conn != null)
				conn.disconnect();
		}
		catch (URISyntaxException e) {			
			e.printStackTrace();
		}
		catch (LDAPException e) {
			// in case we have not succeded in querying information, throw a special exception
			throw new TaskFailedException(this.getResourceURL() + ":" + e.getMessage());
		}
		finally {
			if (conn != null && conn.isConnected())
				try {
					conn.disconnect();
				}
				catch (LDAPException e) {
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
			Attribute temp = new BasicAttribute(current.getName(), current.getStringValueArray()[0]);
			resulting.put(temp);
		}
		return resulting;
	}
	
	public String toString() {
		return "Ldap query work: " + this.getResourceURL() +", " + this.getDn();
	}
}
