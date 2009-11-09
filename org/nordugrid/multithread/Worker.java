package org.nordugrid.multithread;

import org.apache.log4j.Logger;

public class Worker extends Thread {
	private int ALLOWED_TIMES_OF_FAILRUES = 1;
	private static Logger log = Logger.getLogger(Worker.class);

	private WorkQueue q;

	public Worker(ThreadGroup tg, WorkQueue q) {
		super(tg, "worker" + System.currentTimeMillis());
		this.q = q;
	}

	public void run() {
		try {
			while (true) {
				// Retrieve some work; block if the queue is empty, null if no tasks pending
				WorkQueue.QueueEntry x = q.getWork();
				if (x == null)
					break;				
				Object value = x.getValue();
				// Terminate if the end-of-stream marker was retrieved
				if (value instanceof EndOfWorkMarker) {
					break;
				}
				else {
					Task work = (Task) value;
					long start = System.currentTimeMillis();
					try {					
						work.process();
						x.setPriorty(Manager.COMPLETED_PRIORITY);
						x.setTaken(false);
					}
					catch (TaskFailedException e) {
						if (log.isDebugEnabled())
							e.printStackTrace();
						work.setResult(e.getMessage());
						x.increasePriority(); //setPriorty(x.getPriorty() + 1);						
						if (x.getPriorty() > Manager.STARTING_PRIORITY + ALLOWED_TIMES_OF_FAILRUES)
							x.setExecutable(false);
						x.setTaken(false);
					}					
				}
			}
		}
		catch (InterruptedException e) {
			System.err.println(e);
		}
	}

	public static class EndOfWorkMarker {

		public String toString() {
			return "End-of-work marker";
		}
	}

}