package org.nordugrid.is;

import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.nordugrid.is.query.tasks.LDAPQueryAuthorizedQueuesTask;
import org.nordugrid.is.query.tasks.LDAPQueryUserJobsTask;
import org.nordugrid.multithread.Manager;
import org.nordugrid.multithread.WorkQueue;
import org.nordugrid.utils.TaskUtilities;

public final class ARCDiscovery {

	private Set GIISes = new HashSet();

	private Set clusters = new HashSet();

	private Set SEs = new HashSet();

	private final int QUERY_JOB = 1;

	private final int QUERY_AUTH_QUEUES = 2;

	public ARCDiscovery() {
	}

	public ARCDiscovery(String u) {
		if (u != null)
			GIISes.add(u);
	}

	public void addGIIS(String u) {
		if (u == null)
			return;
		GIISes.add(u);
	}

	public void addCluster(String u) {
		if (u == null)
			return;
		clusters.add(u);
	}

	public Set getSEs() {
		return SEs;
	}

	public void setSEs(Set es) {
		SEs = es;
	}

	public Set getClusters() {
		return clusters;
	}

	public void setClusters(Set clusters) {
		this.clusters = clusters;
	}

	public Set getGIISes() {
		return GIISes;
	}

	public void setGIISes(Set ses) {
		GIISes = ses;
	}

	public void discoverAll() {
		// Go through all GIISes
		Set open = new HashSet(GIISes);
		Set closed = new HashSet();

		while (open.size() != 0) {
			String GIIS = (String) (open.iterator().next());
			open.remove(GIIS);
			closed.add(GIIS);

			try {
				// System.err.println("Current = " + GIIS);
				Hashtable env = new Hashtable();
				env.put(Context.INITIAL_CONTEXT_FACTORY,
						"com.sun.jndi.ldap.LdapCtxFactory");
				env.put(Context.PROVIDER_URL, GIIS);
				env.put("com.sun.jndi.ldap.connect.timeout", "10000");
				DirContext ctx = new InitialDirContext(env);
				String[] needed_attrs = { "giisregistrationstatus" };
				SearchControls controls = new SearchControls();
				controls.setCountLimit(0);
				controls.setTimeLimit(5000);
				// controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
				controls.setSearchScope(SearchControls.OBJECT_SCOPE);
				controls.setReturningAttributes(needed_attrs);
				NamingEnumeration resultp = ctx.search("", "(objectclass=*)",
						controls);
				// Each result represents registered GIIS or GRIS
				for (; resultp.hasMore();) {
					SearchResult result = (SearchResult) (resultp.next());
					Attributes attrs = result.getAttributes();
					if (attrs.size() == 0)
						continue;
					StringBuffer url = new StringBuffer();
					try {
						// Reject nonVALID entries
						String value = (String) (attrs.get("Mds-Reg-status")
								.get());
						if (!value.equals("VALID"))
							continue;
						// Make URL
						url.append("ldap://");
						url.append(attrs.get("Mds-Service-hn").get());
						url.append(":");
						url.append(attrs.get("Mds-Service-port").get());
						// Decide where to put it
						String suffix = (String) (attrs
								.get("Mds-Service-Ldap-suffix").get());
						// Strip spaces from parts of DN
						suffix = suffix.replaceAll("\\s", "");
						url.append("/");
						url.append(suffix);
						// System.out.println(suffix);

						// Fixes by Frederik Orellana
						if (suffix.length() >= 17
								&& suffix.substring(0, 17).equalsIgnoreCase(
										"Mds-Vo-name=local")) {
							// Cluster
							addCluster(url.toString());
						} else if (suffix.length() >= 23
								&& suffix.substring(0, 23).equalsIgnoreCase(
										"nordugrid-cluster-name=")) {
							// Cluster
							addCluster(url.toString());
						} else if (suffix.length() >= 18
								&& suffix.substring(0, 18).equalsIgnoreCase(
										"nordugrid-se-name=")) {
							// SE
							SEs.add(url.toString());
						} else if (suffix.length() >= 12
								&& suffix.substring(0, 12).equalsIgnoreCase(
										"Mds-Vo-name=")) {
							// GIIS
							if (closed.contains(url.toString()))
								continue;
							else
								open.add(url.toString());
						}

					} catch (java.lang.Exception err) {
						err.printStackTrace();
						continue;
					}
				}
				ctx.close();
			} catch (NamingException err) {
				err.printStackTrace();
				continue;
			}
		}
		this.GIISes = closed;
	}

	public Collection findAuthorizedResources(String userDN, int nrOfThreads,
			long totalTime) throws InterruptedException {
		return commonQuery(QUERY_AUTH_QUEUES, userDN, nrOfThreads, totalTime);
	}

	public Collection findUserJobs(String userDN, int nrOfThreads,
			long totalTime) throws InterruptedException {
		return commonQuery(QUERY_JOB, userDN, nrOfThreads, totalTime);
	}

	private Collection commonQuery(int queryType, String userDN,
			int nrOfThreads, long totalTime) throws InterruptedException {
		if (clusters == null || clusters.size() == 0)
			return new HashSet();
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		// Add some work to the queue; block if the queue is full.
		// Note that null cannot be added to a blocking queue.
		switch (queryType) {
		case QUERY_JOB:
			for (Iterator it = clusters.iterator(); it.hasNext();) {
				queue.addWork(new LDAPQueryUserJobsTask((String) it.next(),
						userDN, totalTime), Manager.STARTING_PRIORITY);
			}
			break;
		case QUERY_AUTH_QUEUES:
			for (Iterator it = clusters.iterator(); it.hasNext();) {
				queue.addWork(new LDAPQueryAuthorizedQueuesTask((String) it
						.next(), userDN, totalTime), Manager.STARTING_PRIORITY);
			}
			break;
		}
		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}
}
