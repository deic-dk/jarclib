package org.nordugrid.multithread;

import org.apache.log4j.Logger;

/**
 * Abstraction for managing workers and distibuting tasks. Most of the time the manager sleeps and 
 * the whole work is done by the workers. 
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class Manager {
	public static final  int MAX_PRIORITY = -1000000;
	public static final  int STARTING_PRIORITY = 20;
	public static final  int SMALLEST_PRIORITY = 1000000;  
	public static final  int COMPLETED_PRIORITY = 1000002;
	
	private static Logger log = Logger.getLogger(Manager.class);
	
	public static WorkQueue execute(WorkQueue queue, int nrOfThreads, long totalTime) {
		long timeStarted = System.currentTimeMillis();
		boolean workForever = false;
		if (totalTime <= 0) 
			workForever = true;
		
		if (nrOfThreads <= 0)
			nrOfThreads = 1;
		// Create the work queue		

		// Create a set of worker threads
		Worker[] workers = new Worker[nrOfThreads];
		ThreadGroup workGroup = new ThreadGroup("searchers");		
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new Worker(workGroup, queue);
			workers[i].start();
		}

		// Add special end-of-stream markers to terminate the workers
		for (int i = 0; i < workers.length; i++) {
			queue.addWork(new Worker.EndOfWorkMarker(), SMALLEST_PRIORITY);
		}

		int timeToSleep = 500;
		while (true) {
			//log.debug("Treads remaining: " + workGroup.activeCount());
			try {
				long elapsed = System.currentTimeMillis() - timeStarted;
				if (workForever || elapsed < totalTime) {
					if (!workForever)
						//log.debug(" ........ time remaining:" + (totalTime - elapsed));
            System.out.println(" ........ time remaining:" + totalTime+" - "+elapsed +"="+(totalTime - elapsed)+
                " --> "+workGroup.activeCount());
					Thread.sleep(timeToSleep);
				}					
				else {
					for (int i = 0; i < workGroup.activeCount(); i++) {
						queue.addWork(new Worker.EndOfWorkMarker(), MAX_PRIORITY);
					}
          System.out.println("Returning work queue");
					return queue;
				}
        if(workGroup.activeCount() == 0){
          System.out.println("No more active threads, returning "+" --> "+workGroup.activeCount());
          break;
        }
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return queue;
	}
}
