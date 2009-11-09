package org.nordugrid.gridftp.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.globus.gsi.GlobusCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.multithread.TaskFailedException;

public final class GridFTPSubmitTask extends GridFTPBaseTask {

	private String url;

	private GlobusCredential credentials;

	private String proxyLocation;

	private String xrsl;

	private List files;

	private List names;

	public GridFTPSubmitTask(GlobusCredential credentials, String url, String xrsl) {
		super();
		this.credentials = credentials;
		this.url = url;
		this.xrsl = xrsl;
	}

	public GridFTPSubmitTask(GlobusCredential credentials, List files, List names, String url, String xrsl) {
		super();
		this.credentials = credentials;
		this.files = files;
		this.names = names;
		this.url = url;
		this.xrsl = xrsl;
	}

	public GridFTPSubmitTask(String proxyLocation, String url, String xrsl) {
		super();
		this.proxyLocation = proxyLocation;
		this.url = url;
		this.xrsl = xrsl;
	}

	public GridFTPSubmitTask(String proxyLocation, List files, List names, String url, String xrsl) {
		super();
		this.files = files;
		this.names = names;
		this.proxyLocation = proxyLocation;
		this.url = url;
		this.xrsl = xrsl;
	}

	public void process() throws TaskFailedException {
		ARCGridFTPJob job = null;
		try {
			job = new ARCGridFTPJob(url);

			// It might be reasonable to throw an exception no credential related
			// information
			// has been specified. But let's delegate the check to the lower layer.
			if (this.credentials != null) {
				job.addProxy(this.credentials);
			}
			else if (this.proxyLocation != null) {
				job.addProxyLocation(this.proxyLocation);
			}
			job.connect();
			if (this.names != null && this.files != null) {
				job.submit(this.xrsl, files, names);
				Collection result = new ArrayList();
				result.add(job.getGlobalId());
				setResult(result);
			}
			else {
				job.submit(this.xrsl);				
				setResult(job.getGlobalId());
			}
		}
		catch (ARCGridFTPJobException e) {
			throw new TaskFailedException(e.getMessage());
		}
		finally {
			if (job != null)
				job.disconnect();
		}
	}
	
	public String toString() {
		return "Job description: " + xrsl;
	}
}
