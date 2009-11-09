package org.nordugrid.gridftp.tasks;

import org.nordugrid.multithread.Task;

public abstract class GridFTPBaseTask implements Task {

	private Object result;
	
	public Object getResult() {
		return result;
	}
	
	public void setResult(Object result) {
		this.result = result;
	}

}
