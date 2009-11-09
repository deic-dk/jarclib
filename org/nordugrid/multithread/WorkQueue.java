package org.nordugrid.multithread;

import java.util.Iterator;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.PriorityBuffer;

/**
 * Implementation of the priority queue using PriorityBuffer.
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class WorkQueue {
	private Buffer queue = new PriorityBuffer();
	

	// Add work to the work queue
	public synchronized void addWork(Object o, int priority) {
		// System.out.println("Adding work" + o + ", priority = " + priority);
		queue.add(new QueueEntry(o, priority));
		notify();
	}

	// Retrieve work from the work queue; block if the queue is empty
	public synchronized WorkQueue.QueueEntry getWork() throws InterruptedException {
		while (queue.isEmpty()) {
			wait();
		}	
		Iterator it = queue.iterator();
		while (it.hasNext()) {
			WorkQueue.QueueEntry current = (WorkQueue.QueueEntry) it.next();
			if (current.isTaken() || !current.isExecutable() || current.getPriorty() == Manager.COMPLETED_PRIORITY)
				continue;
			else  {
				current.setTaken(true);
				return current;
			}
				
		}
		// hopefully this part will never be reached :)
		return null;
		
	}
	
	/*
	 * Careful with that function, not syncronized (for a reason!)
	 */
	public Iterator iterator() {
		return queue.iterator();
	}

	public class QueueEntry implements Comparable {

		private Object value;

		private int priorty;
		
		private boolean isExecutable = true;
		
		private boolean taken;

		public QueueEntry(Object value, int priorty) {
			super();
			this.value = value;
			this.priorty = priorty;
		}

		public void increasePriority() {
			this.priorty++;
		}
		
		public int compareTo(Object o) {
			QueueEntry entry = (QueueEntry) o;
			return this.priorty - entry.getPriorty();
		}

		public int getPriorty() {
			return priorty;
		}

		public void setPriorty(int priorty) {
			this.priorty = priorty;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public boolean isTaken() {
			return taken;
		}

		public void setTaken(boolean taken) {
			this.taken = taken;
		}

		public boolean isExecutable() {
			return isExecutable;
		}

		public void setExecutable(boolean isExecutable) {
			this.isExecutable = isExecutable;
		}			
		
	}

	public int size() {
		return queue.size();
	}
	
	
	
}