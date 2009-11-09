package org.nordugrid.gridftp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.globus.gsi.GlobusCredential;
import org.globus.rsl.AbstractRslNode;
import org.globus.rsl.ParseException;
import org.globus.rsl.RSLParser;
import org.globus.rsl.RslNode;
import org.nordugrid.gridftp.tasks.GridFTPCancelTask;
import org.nordugrid.gridftp.tasks.GridFTPCleanTask;
import org.nordugrid.gridftp.tasks.GridFTPGetTask;
import org.nordugrid.gridftp.tasks.GridFTPListTask;
import org.nordugrid.gridftp.tasks.GridFTPSubmitTask;
import org.nordugrid.multithread.Manager;
import org.nordugrid.multithread.WorkQueue;
import org.nordugrid.utils.TaskUtilities;

public class ARCGridFTPMultipleJobs {

	public static List submit(String proxyLocation, List xrsls, List urls, int nrOfThreads, long totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualSubmit(proxyLocation, null, xrsls, urls, nrOfThreads, totalTime);
	}

	public static List submit(GlobusCredential proxy, List xrsls, List urls, int nrOfThreads, long totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualSubmit(null, proxy, xrsls, urls, nrOfThreads, totalTime);
	}

	public static List submit(GlobusCredential proxy, String multiXRSL, String url, int nrOfThreads, long totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		RslNode root;
		try {
			root = RSLParser.parse(multiXRSL);
			return submit(proxy, root, url, nrOfThreads, totalTime);
		}
		catch (ParseException e) {
			throw new ARCGridFTPJobException("Failed to parse multiXRSL description");
		}
	}

	public static List submit(GlobusCredential proxy, RslNode multiXRSL, String url, int nrOfThreads, long totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		List xrsls = new ArrayList();
		List urls = new ArrayList();
		if (multiXRSL.getOperator() != AbstractRslNode.MULTI) {
			xrsls.add(multiXRSL.toRSL(true));
			urls.add(url);
			return actualSubmit(null, proxy, xrsls, urls, nrOfThreads, totalTime);
		}
		else {
			
			for (Iterator it = multiXRSL.getSpecifications().iterator(); it.hasNext();) {
				xrsls.add(((RslNode) it.next()).toRSL(true));
				urls.add(url);
			}
			return actualSubmit(null, proxy, xrsls, urls, nrOfThreads, totalTime);
		}
	}

	public static List submit(String proxyLocation, List filesList, List namesList, List xrsls, List urls, int nrOfThreads,
			long totalTime) throws ARCGridFTPJobException, InterruptedException {
		return actualSubmitWithUpload(proxyLocation, null, filesList, namesList, xrsls, urls, nrOfThreads, totalTime);
	}

	public static List submit(GlobusCredential proxy, List filesList, List namesList, List xrsls, List urls, int nrOfThreads,
			long totalTime) throws ARCGridFTPJobException, InterruptedException {
		return actualSubmitWithUpload(null, proxy, filesList, namesList, xrsls, urls, nrOfThreads, totalTime);
	}

	public static List cancel(String proxyLocation, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualCancel(proxyLocation, null, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List cancel(GlobusCredential proxy, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualCancel(null, proxy, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List clean(String proxyLocation, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualClean(proxyLocation, null, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List clean(GlobusCredential proxy, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualClean(null, proxy, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List clean(String proxyLocation, List globalJobIds, int nrOfThreads, int totalTime) throws ARCGridFTPJobException, InterruptedException {
		List urls = new ArrayList();
		List jobIds = new ArrayList();
		for (Iterator it = globalJobIds.iterator(); it.hasNext();) {
			String url = (String) it.next();
			int loc = url.lastIndexOf('/');
			urls.add(url.substring(0, loc));
			jobIds.add(url.substring(loc + 1, url.length()));
		}
		return actualClean(proxyLocation, null, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List clean(GlobusCredential proxy, List globalJobIds, int nrOfThreads, int totalTime) throws ARCGridFTPJobException, InterruptedException {
		List urls = new ArrayList();
		List jobIds = new ArrayList();
		for (Iterator it = globalJobIds.iterator(); it.hasNext();) {
			String url = (String) it.next();
			int loc = url.lastIndexOf('/');
			urls.add(url.substring(0, loc));
			jobIds.add(url.substring(loc + 1, url.length()));
		}
		return actualClean(null, proxy, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List list(String proxyLocation, List jobIds, List urls, int nrOfThreads, int totalTime) throws ARCGridFTPJobException, InterruptedException {
		return actualList(proxyLocation, null, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List list(GlobusCredential proxy, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualList(null, proxy, jobIds, urls, nrOfThreads, totalTime);
	}

	public static List get(String proxyLocation, List jobIds, List urls, List localDirs, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualGet(proxyLocation, null, jobIds, urls, localDirs, nrOfThreads, totalTime);
	}

	public static List get(GlobusCredential proxy, List jobIds, List urls, List localDirs, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		return actualGet(null, proxy, jobIds, urls, localDirs, nrOfThreads, totalTime);
	}

	public static List get(String proxyLocation, List globalJobIds, List localDirs, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		List urls = new ArrayList();
		List jobIds = new ArrayList();
		for (Iterator it = globalJobIds.iterator(); it.hasNext();) {
			String url = (String) it.next();
			int loc = url.lastIndexOf('/');
			urls.add(url.substring(0, loc));
			jobIds.add(url.substring(loc + 1, url.length()));
		}
		return actualGet(proxyLocation, null, jobIds, urls, localDirs, nrOfThreads, totalTime);
	}

	public static List get(GlobusCredential proxy, List globalJobIds, List localDirs, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		List urls = new ArrayList();
		List jobIds = new ArrayList();
		for (Iterator it = globalJobIds.iterator(); it.hasNext();) {
			String url = (String) it.next();
			int loc = url.lastIndexOf('/');
			urls.add(url.substring(0, loc));
			jobIds.add(url.substring(loc + 1, url.length()));
		}
		return actualGet(null, proxy, jobIds, urls, localDirs, nrOfThreads, totalTime);
	}

	//	
	// Functions acting as a facade to the actual execution.
	// TODO: refactor the class design
private static List actualSubmit(String proxyLocation, GlobusCredential proxy, List xrsls, List urls, int nrOfThreads,
			long totalTime) throws ARCGridFTPJobException, InterruptedException {
		if (xrsls.size() != urls.size())
			throw new ARCGridFTPJobException("Lists of job description and potential targets have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator xrslIterator = xrsls.iterator();
		Iterator urlsIterator = urls.iterator();

		if (proxy != null) {
			while (xrslIterator.hasNext()) {
				queue.addWork(new GridFTPSubmitTask(proxy, urlsIterator.next().toString(), xrslIterator.next().toString()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (xrslIterator.hasNext()) {
				queue.addWork(new GridFTPSubmitTask(proxyLocation, urlsIterator.next().toString(),  xrslIterator.next().toString()),
						Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}
		
		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}
	private static List actualSubmitWithUpload(String proxyLocation, GlobusCredential proxy, List filesList, List namesList,
			List xrsls, List urls, int nrOfThreads, long totalTime) throws ARCGridFTPJobException, InterruptedException {
		if (xrsls.size() != urls.size())
			throw new ARCGridFTPJobException("Lists of job description and potential targets have different sizes!");

		if (filesList.size() != namesList.size())
			throw new ARCGridFTPJobException("Lists of lists of file names and files have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator xrslIterator = xrsls.iterator();
		Iterator urlsIterator = urls.iterator();
		Iterator filesIterator = filesList.iterator();
		Iterator namesListIterator = namesList.iterator();

		if (proxy != null) {
			while (xrslIterator.hasNext()) {
				queue.addWork(new GridFTPSubmitTask(proxy, (List) filesIterator.next(), (List) namesListIterator.next(), (String) urlsIterator
						.next(), xrslIterator.next().toString()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (xrslIterator.hasNext()) {
				queue.addWork(new GridFTPSubmitTask(proxyLocation, (List) filesIterator.next(), (List) namesListIterator.next(),
						(String) urlsIterator.next(), (String) xrslIterator.next()), Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}

		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}

	private static List actualCancel(String proxyLocation, GlobusCredential proxy, List jobIds, List urls, int nrOfThreads,
			int totalTime) throws ARCGridFTPJobException, InterruptedException {
		if (jobIds.size() != urls.size())
			throw new ARCGridFTPJobException("Lists of job ids and hosts have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator jobsIterator = jobIds.iterator();
		Iterator urlsIterator = urls.iterator();

		if (proxy != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPCancelTask(proxy, (String) urlsIterator.next(), (String) jobsIterator.next()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPCancelTask(proxyLocation, (String) urlsIterator.next(), (String) jobsIterator.next()),
						Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}

		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}

	private static List actualClean(String proxyLocation, GlobusCredential proxy, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		if (jobIds.size() != urls.size())
			throw new ARCGridFTPJobException("Lists of job ids and hosts have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator jobsIterator = jobIds.iterator();
		Iterator urlsIterator = urls.iterator();

		if (proxy != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPCleanTask(proxy, (String) urlsIterator.next(), (String) jobsIterator.next()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPCleanTask(proxyLocation, (String) urlsIterator.next(), (String) jobsIterator.next()),
						Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}

		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}

	private static List actualList(String proxyLocation, GlobusCredential proxy, List jobIds, List urls, int nrOfThreads, int totalTime)
			throws ARCGridFTPJobException, InterruptedException {
		if (jobIds.size() != urls.size())
			throw new ARCGridFTPJobException("Lists of job ids and hosts have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator jobsIterator = jobIds.iterator();
		Iterator urlsIterator = urls.iterator();

		if (proxy != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPListTask(proxy, (String) urlsIterator.next(), (String) jobsIterator.next()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPListTask(proxyLocation, (String) urlsIterator.next(), (String) jobsIterator.next()),
						Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}
		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}

	private static List actualGet(String proxyLocation, GlobusCredential proxy, List jobIds, List urls, List localDirs,
			int nrOfThreads, int totalTime) throws ARCGridFTPJobException, InterruptedException {
		if (jobIds.size() != urls.size() && jobIds.size() != localDirs.size())
			throw new ARCGridFTPJobException("Lists of job ids, hosts and local directories have different sizes!");
		// Create the work queue
		WorkQueue queue = new WorkQueue();
		Iterator jobsIterator = jobIds.iterator();
		Iterator urlsIterator = urls.iterator();
		Iterator localDirsIterator = localDirs.iterator();

		if (proxy != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPGetTask(proxy, (String) urlsIterator.next(), (String) jobsIterator.next(), (String) localDirsIterator
						.next()), Manager.STARTING_PRIORITY);
			}
		}
		else if (proxyLocation != null) {
			while (jobsIterator.hasNext()) {
				queue.addWork(new GridFTPGetTask(proxyLocation, (String) urlsIterator.next(), (String) jobsIterator.next(),
						(String) localDirsIterator.next()), Manager.STARTING_PRIORITY);
			}
		}
		else {
			throw new ARCGridFTPJobException("You have to define at least one of two: Proxy object or proxy file location");
		}

		Manager.execute(queue, nrOfThreads, totalTime);
		return TaskUtilities.convertQueueToTaskResultList(queue);
	}


}
