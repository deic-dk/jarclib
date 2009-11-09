package org.nordugrid.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.nordugrid.multithread.Manager;
import org.nordugrid.multithread.Task;
import org.nordugrid.multithread.TaskResult;
import org.nordugrid.multithread.WorkQueue;
import org.nordugrid.multithread.Worker;
import org.nordugrid.multithread.WorkQueue.QueueEntry;

public class TaskUtilities {
	/**
	 * @return List <WorkResult>
	 * @throws InterruptedException
	 */
	public  static List convertQueueToTaskResultList(WorkQueue wq) throws InterruptedException {
		List resultList = new ArrayList();
		Iterator it = wq.iterator();
		while (it.hasNext()) {
			QueueEntry next = (QueueEntry) it.next();
			// skip all the end-of-work markers as jarclib specific
			if (next.getValue() instanceof Worker.EndOfWorkMarker)
				continue;

			Task t = (Task) next.getValue();
			TaskResult tr = new TaskResult();
			switch (next.getPriorty()) {
			case Manager.STARTING_PRIORITY:
				tr.setStatus(TaskResult.STATUS_NOT_RUN);
				break;
			case Manager.COMPLETED_PRIORITY:
				tr.setStatus(TaskResult.STATUS_COMPLETED);
				break;
			default:
				tr.setStatus(TaskResult.STATUS_FAILED);
				break;
			}
			tr.setResult(t.getResult());
			tr.setWorkDescription(t.toString());
			resultList.add(tr);
		}
		return resultList;
	}
}
