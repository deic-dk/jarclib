package org.nordugrid.multithread;

/**
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class TaskFailedException extends Exception {
	public TaskFailedException() {
		super();		
	}
	
	public TaskFailedException(String msg) {
		super(msg);		
	}
}
