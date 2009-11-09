package org.nordugrid.gridftp.tasks;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.log4j.Logger;
import org.globus.gsi.GlobusCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.multithread.TaskFailedException;

public final class GridFTPGetTask extends GridFTPBaseTask {

	private static Logger log = Logger.getLogger(GridFTPGetTask.class);
	
	private String url;

	private GlobusCredential credentials;
	
	private String jobId;

	private String proxyLocation;
	
	private String localDir;

	public GridFTPGetTask(GlobusCredential credentials, String url, String jobId, String localDir) {
		super();
		this.credentials = credentials;
		this.url = url;
		this.jobId = jobId;
		this.localDir = localDir;
	}

	public GridFTPGetTask(String proxyLocation, String url, String jobId, String localDir) {
		super();
		this.proxyLocation = proxyLocation;
		this.url = url;
		this.jobId = jobId;
		this.localDir = localDir;
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
			Collection result = new ArrayList(1);
			log.info("Downloading to " + localDir);
			job.get(localDir);
			result.add("Job " + jobId + " output downloaded to '" + localDir + "'"); 
			setResult(result);			
		}
		catch (ARCGridFTPJobException e) {
			if (log.isDebugEnabled()) {
				e.printStackTrace();
				log.debug("Downloading to folder:" + localDir);
			}
			throw new TaskFailedException(e.getMessage());
		}
		finally {
			if (job != null)
				job.disconnect();
		}
	}
	
	public String toString() {
		return "Downloading output of '" + url + jobId + "' to " + localDir; 
	}
}
