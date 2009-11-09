package org.nordugrid.is.query.tasks;

import org.nordugrid.multithread.Task;

public abstract class QueryTask implements Task {

	private String dn;

	private String resourceURL;

	private Object result;

	public QueryTask(String clusterURL, String dn) {
		super();
		this.resourceURL = clusterURL;
		this.dn = dn;
	}

	public String getResourceURL() {
		return resourceURL;
	}

	public void setResourceURL(String clusterURL) {
		this.resourceURL = clusterURL;
	}

	public String getDn() {
		return dn;
	}

	public void setDn(String dn) {
		this.dn = dn;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}


	
}
