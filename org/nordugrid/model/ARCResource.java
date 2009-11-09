package org.nordugrid.model;

import java.util.HashSet;
import java.util.Set;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

public class ARCResource {

	private String mdsurl = null;

	private String contactString = null;

	private String queueName = null;

	private String clusterName = null;

	private int freejobs = -1;

	private int maxjobs = -1;

	private String architecture = null;

	private String middleware = null;

	private String opsys = null;

	private String nodeCPU = null;

	private int totalQueueCPUs = -1;

	private int totalClusterCPUs = -1;

	// private List runtimeenvironment = new LinkedList();
	private Set runtimeenvironment = new HashSet();

	// private List nodeAccess = new LinkedList();
	private Set nodeAccess = new HashSet();

	private int diskspace = 0;

	private int nodememory = 0;

	private int maxCpuTime = -1;

	private int minCpuTime = -1;

	private int queued;

	private String clusterAlias = null;

	public ARCResource() {
	}

	private String setValue(String name, Attributes attrs, String value) {
		try {
			// Fix by Frederik Orellana
			// String v = (String) (attrs.get(name).get());
			String v = (String) ((HashSet) (attrs.get(name).get())).iterator()
					.next();
			if (v != null)
				value = v;
		} catch (java.lang.Exception err) {
		}
		return value;
	}

	private int setValue(String name, Attributes attrs, int value) {
		try {
			String v = setValue(name, attrs, null);
			Integer n = new Integer(v);
			value = n.intValue();
		} catch (Exception err) {
		}
		return value;
	}

	private void setValues(String name, Attributes attrs, Set values) {
		try {
			Attribute attr = attrs.get(name);
			for (int i = 0; i < attr.size(); i++) {				
				HashSet v = (HashSet) (attrs.get(name).get(i));
				if (v == null)
					continue;
				values.addAll(v);
			}
		} catch (Exception err) {
		}
	}

	private void parseAttributes(Attributes cluster, Attributes queue,
			Attributes user) {

		architecture = setValue("nordugrid-cluster-architecture", cluster,
				architecture);
		middleware = setValue("nordugrid-cluster-middleware", cluster,
				middleware);
		opsys = setValue("nordugrid-cluster-opsys", cluster, opsys);
		nodeCPU = setValue("nordugrid-cluster-nodecpu", cluster, nodeCPU);
		clusterAlias = setValue("nordugrid-cluster-aliasname", cluster,
				clusterAlias);
		totalQueueCPUs = setValue("nordugrid-queues-totalcpus", queue,
				totalQueueCPUs);
		totalQueueCPUs = setValue("nordugrid-cluster-totalcpus", cluster,
				totalClusterCPUs);
		queued = setValue("nordugrid-queue-totalcpus", queue, queued);
		setValues("nordugrid-cluster-runtimeenvironment", cluster,
				runtimeenvironment);

		architecture = setValue("nordugrid-queue-architecture", cluster,
				architecture);
		middleware = setValue("nordugrid-queue-middleware", cluster, middleware);
		opsys = setValue("nordugrid-queue-opsys", cluster, opsys);
		setValues("nordugrid-queue-runtimeenvironment", cluster,
				runtimeenvironment);

		architecture = setValue("nordugrid-authuser-architecture", cluster,
				architecture);
		middleware = setValue("nordugrid-authuser-middleware", cluster,
				middleware);
		opsys = setValue("nordugrid-authuser-opsys", cluster, opsys);
		setValues("nordugrid-authuser-runtimeenvironment", cluster,
				runtimeenvironment);

		contactString = setValue("nordugrid-cluster-contactstring", cluster,
				contactString);
		queueName = setValue("nordugrid-queue-name", queue, queueName);
		clusterName = setValue("nordugrid-cluster-name", cluster, clusterName);

		diskspace = setValue("nordugrid-cluster-sessiondir-free", cluster,
				diskspace);
		nodememory = setValue("nordugrid-queue-nodememory", queue, nodememory);

		setValues("nordugrid-cluster-nodeaccess", cluster, nodeAccess);
		
		int cpus = -1;
		int jobs = -1;
		int used = -1;
		int queued = -1;
		cpus = setValue("nordugrid-cluster-totalcpus", cluster, cpus);
		jobs = setValue("nordugrid-cluster-totaljobs", cluster, jobs);
		used = setValue("nordugrid-cluster-usedcpus", cluster, used);
		queued = setValue("nordugrid-cluster-queuedjobs", cluster, queued);
		if ((queued >= 0) && (used >= 0) && ((queued + used) > jobs))
			jobs = queued + used;
		if ((jobs >= 0) && (cpus >= 0)) {
			if (jobs > cpus) {
				freejobs = 0;
			} else {
				freejobs = cpus - jobs;
			}
		}

		used = -1;
		int maxused = -1;
		queued = -1;
		int maxqueued = -1;
		used = setValue("nordugrid-queue-running", queue, used);
		maxused = setValue("nordugrid-queue-maxrunning", queue, maxused);
		queued = setValue("nordugrid-queue-queued", queue, queued);
		maxqueued = setValue("nordugrid-queue-maxqueuable", queue, maxqueued);
		if ((cpus >= 0) && (cpus < maxused))
			maxused = cpus;
		cpus = -1;
		cpus = setValue("nordugrid-queue-totalcpus", queue, cpus);
		if ((cpus >= 0) && (cpus < maxused))
			maxused = cpus;
		cpus = setValue("nordugrid-queue-maxuserrun", queue, cpus);
		if ((cpus >= 0) && (cpus < maxused))
			maxused = cpus;
		if ((used >= 0) && (queued >= 0))
			used += queued;
		if ((maxused >= 0) && (used >= 0) && (maxused > used)) {
			if (((maxused - used) < freejobs) || (freejobs < 0))
				freejobs = maxused - used;
		}
		if ((maxused >= 0) && (maxqueued >= 0)) {
			if (((maxused + maxqueued) < maxjobs) || (maxjobs < 0))
				maxjobs = (maxused + maxqueued);
		}

		int freecpu = -1;
		freecpu = setValue("nordugrid-authuser-freecpus", user, freecpu);
		if ((freecpu >= 0) && ((freecpu < freejobs) || (freejobs < 0)))
			freejobs = freecpu;

		if (freecpu < 0)
			freecpu = 0;

		maxCpuTime = setValue("nordugrid-queue-maxcputime", queue, maxCpuTime);
		minCpuTime = setValue("nordugrid-queue-mincputime", queue, minCpuTime);
	}

	// JNDI version
	public ARCResource(Attributes cluster, Attributes queue, Attributes user) {
		parseAttributes(cluster, queue, user);
	}

	public String getArchitecture() {
		return architecture;
	}

	public void setArchitecture(String architecture) {
		this.architecture = architecture;
	}

	public int getDiskspace() {
		return diskspace;
	}

	public void setDiskspace(int diskspace) {
		this.diskspace = diskspace;
	}

	public int getFreejobs() {
		return freejobs;
	}

	public void setFreejobs(int freejobs) {
		this.freejobs = freejobs;
	}

	public String getContactString() {
		return contactString;
	}

	public void setContactString(String joburl) {
		this.contactString = joburl;
	}

	public int getMaxjobs() {
		return maxjobs;
	}

	public void setMaxjobs(int maxjobs) {
		this.maxjobs = maxjobs;
	}

	public String getMdsurl() {
		return mdsurl;
	}

	public void setMdsurl(String mdsurl) {
		this.mdsurl = mdsurl;
	}

	public String getMiddleware() {
		return middleware == null ? "Unknown" : middleware;
	}

	public void setMiddleware(String middleware) {
		this.middleware = middleware;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String name) {
		this.queueName = name;
	}

	public int getNodememory() {
		return nodememory;
	}

	public void setNodememory(int nodememory) {
		this.nodememory = nodememory;
	}

	public String getOpsys() {
		return opsys;
	}

	public void setOpsys(String opsys) {
		this.opsys = opsys;
	}


	public String getNodeCPU() {
		return nodeCPU == null ? "Unknown" : nodeCPU;
	}

	public void setNodeCPU(String nodeCPU) {
		this.nodeCPU = nodeCPU;
	}

	public int getTotalClusterCPUs() {
		return totalClusterCPUs;
	}

	public void setTotalClusterCPUs(int totalClusterCPUs) {
		this.totalClusterCPUs = totalClusterCPUs;
	}

	public int getTotalQueueCPUs() {
		return totalQueueCPUs;
	}

	public void setTotalQueueCPUs(int totalQueueCPUs) {
		this.totalQueueCPUs = totalQueueCPUs;
	}

	public String getClusterAlias() {
		return clusterAlias;
	}

	public void setClusterAlias(String clusterAlias) {
		this.clusterAlias = clusterAlias;
	}

	public int getQueued() {
		return queued;
	}

	public void setQueued(int queued) {
		this.queued = queued;
	}

	public String getClusterName() {
		return clusterName == null ? "Unknown" : clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public Set getNodeAccess() {
		return nodeAccess;
	}

	public void setNodeAccess(Set nodeAccess) {
		this.nodeAccess = nodeAccess;
	}

	public Set getRuntimeenvironment() {
		return runtimeenvironment;
	}

	public void setRuntimeenvironment(Set runtimeenvironment) {
		this.runtimeenvironment = runtimeenvironment;
	}

	public int getMaxCpuTime() {
		return maxCpuTime != -1 ? maxCpuTime : Integer.MAX_VALUE;
	}

	public void setMaxCpuTime(int maxCpuTime) {
		this.maxCpuTime = maxCpuTime;
	}

	public int getMinCpuTime() {
		return minCpuTime != -1 ? minCpuTime : Integer.MIN_VALUE;
	}

	public void setMinCpuTime(int minCpuTime) {
		this.minCpuTime = minCpuTime;
	}

	public String toString() {
		return "ARC Resource:\n " + "Cluster: " + clusterName + "\nQueue: "
				+ queueName + "\nMiddleware: " + this.middleware;
	}

	public int hashCode() {
		return clusterName.hashCode() + queueName.hashCode();
	}

	public boolean equals(Object o) {
		if (!(o instanceof ARCResource))
			return false;
		else {
			ARCResource res = (ARCResource) o;
			return res.getClusterName().equals(this.clusterName)
					&& res.queueName.equals(this.queueName);
		}
	}
}
