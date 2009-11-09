package org.nordugrid.gridftp.tasks;

import org.globus.gsi.GlobusCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.multithread.TaskFailedException;

public final class GridFTPListTask  extends GridFTPBaseTask {

	private String url;

	private GlobusCredential credentials;
	
	private String jobId;

	private String proxyLocation;

	public GridFTPListTask(GlobusCredential credentials, String url, String jobId) {
		super();
		this.credentials = credentials;
		this.url = url;
		this.jobId = jobId;
	}

	public GridFTPListTask(String proxyLocation, String url, String jobId) {
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
			setResult(job.list());
		}
		catch (ARCGridFTPJobException e) {
			e.printStackTrace();
			throw new TaskFailedException(e.getMessage());
		}
		finally {
			if (job != null)
				job.disconnect();
		}
	}
}
