package org.nordugrid.gridftp.tasks;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.log4j.Logger;
import org.globus.gsi.GlobusCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.multithread.TaskFailedException;

public final class GridFTPCancelTask extends GridFTPBaseTask {
	
	private static final Logger log = Logger.getLogger(GridFTPCancelTask.class);

	private String url;

	private GlobusCredential credentials;

	private String proxyLocation;
	
	private String jobId;

	public GridFTPCancelTask(GlobusCredential credentials, String url, String jobId) {
		super();
		this.credentials = credentials;
		this.url = url;
		this.jobId = jobId;
	}

	public GridFTPCancelTask(String proxyLocation, String url, String jobId) {
		super();
		this.proxyLocation = proxyLocation;
		this.url = url;
		this.jobId = jobId;
	}

	public void process() throws TaskFailedException {
		ARCGridFTPJob job = null;
		try {
			job = new ARCGridFTPJob(url, jobId);
			
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
			job.cancel();
			Collection result = new ArrayList();
			result.add(job.getId());
			setResult("Job '" + result + "' cancelled");
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
		return "Cancellation of '" + url + jobId + "'"; 
	}
}
