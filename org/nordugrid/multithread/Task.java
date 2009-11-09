package org.nordugrid.multithread;


/**
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 *
 */
public interface Task{
	
	/**
	 * Process some abstract work and return resutl as a <code>Collection</code>.
	 */
	void process() throws TaskFailedException;
	Object getResult();	
	void setResult(Object result);	
}
