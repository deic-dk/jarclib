package tests;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.globus.common.CoGProperties;
import org.globus.gsi.GlobusCredential;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.gridftp.ARCGridFTPMultipleJobs;
import org.nordugrid.multithread.TaskResult;
import org.nordugrid.utils.CredentialsUtilities;

public class ARCGridFTPTest extends TestCase {
	private Properties p;

	private GlobusCredential gc;
	
	private static Logger log = Logger.getLogger(ARCGridFTPTest.class);

	public void setUp() throws Exception {
		p = new Properties();
		p.put("foo", "bar");		
		p.load(new FileInputStream("test.properties"));
		CoGProperties.getDefault().setCaCertLocations(p.getProperty("ca.certs"));
		gc = CredentialsUtilities.createProxy(p.getProperty("userkey.location"), p.getProperty("usercert.location"), p
				.getProperty("userkey.passphrase"), 120000, 512);
	}

	public void testMultipleSubmitAndClean() throws ARCGridFTPJobException, InterruptedException {
		String job = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" + "(action=request)(arguments=\"/bin/echo\" \"Test\")"
				+ "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" + p.getProperty("test.queue") + "\")";
		List jobs = new ArrayList();
		jobs.add(job);
		jobs.add(job);
		List urls = new ArrayList();
		String url = p.getProperty("test.cluster.submit.url");
		urls.add(url);
		urls.add(url);
		long limit = 10000;
	  long offset = 2000; // some +- coefficient
		long start = System.currentTimeMillis();
		Collection ids = ARCGridFTPMultipleJobs.submit(gc, jobs, urls, 2, limit);
		long end = System.currentTimeMillis();
		assertTrue("Failed to stay within time limit", (end - start) < limit + offset);
		Logger.getRootLogger().setLevel(Level.INFO);
		for (Iterator it = ids.iterator(); it.hasNext();) {
			log.info("Submitted job with id: " + ((TaskResult)it.next()).getResult());
		}
		assertTrue("Failed to submit one or more jobs", ids.size() == 2);
		
		log.debug("Initiating the multiple clean tasks");
		List jobIds = new ArrayList();
		for (Iterator it = ids.iterator(); it.hasNext();) {
			jobIds.add(((TaskResult)it.next()).getResult());
		}
		Collection ids2 = ARCGridFTPMultipleJobs.clean(gc,jobIds, 1, 0);
		for (Iterator it = ids2.iterator(); it.hasNext();) {
			log.debug("Cancelled job with id: " + it.next());
		}
		assertTrue("Faile to clean one or more jobs", ids2.size() == 2);		
	}
	
	public void testMultipleSubmitAndGet() throws ARCGridFTPJobException, InterruptedException {
		String job = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" + "(action=request)(arguments=\"/bin/echo\" \"Test\")"
				+ "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" + p.getProperty("test.queue") + "\")";
		List jobs = new ArrayList();
		jobs.add(job);
		jobs.add(job);
		List urls = new ArrayList();
		String url = p.getProperty("test.cluster.submit.url");
		urls.add(url);
		urls.add(url);
		long limit = 10000;
	  long offset = 2000; // some +- coefficient
		long start = System.currentTimeMillis();
		Collection ids = ARCGridFTPMultipleJobs.submit(gc, jobs, urls, 2, limit);
		long end = System.currentTimeMillis();
		assertTrue("Failed to stay within time limit", (end - start) < limit + offset);
		//Logger.getRootLogger().setLevel(Level.DEBUG);
		for (Iterator it = ids.iterator(); it.hasNext();) {
			log.debug("Submitted job with id: " + ((TaskResult)it.next()).getResult());
		}
		assertTrue("Faile to submit one or more jobs", ids.size() == 2);
		
		log.debug("Initiating the multiple get tasks");
		List dirs = new ArrayList(2);
		dirs.add("test1");
		dirs.add("test2");
		try {
			Thread.sleep(10000); // give it some time to process the data 
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		List jobIds = new ArrayList();
		for (Iterator it = ids.iterator(); it.hasNext();) {
			jobIds.add(((TaskResult)it.next()).getResult());
		}
		Collection ids2 = ARCGridFTPMultipleJobs.get(gc, jobIds, dirs, 1, 10000);
		for (Iterator it = ids2.iterator(); it.hasNext();) {
			log.debug("Got job with id: " + it.next());
		}
		assertTrue("Faile to get one or more jobs", ids2.size() == 2);		
	}

}
