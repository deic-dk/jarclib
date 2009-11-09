package org.nordugrid.multithread;

/**
 * Encapsulates the result of processing the task.
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class TaskResult {
	public static final String STATUS_COMPLETED = "Completed";
	public static final String STATUS_FAILED = "Failed";
	public static final String STATUS_NOT_RUN = "Not run";
	
	private String workDescription;
	private String status;
	private Object result;
	private String comment;
	public String getComment() {
		return comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}
	public Object getResult() {
		return result;
	}
	public void setResult(Object result) {
		this.result = result;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getWorkDescription() {
		return workDescription;
	}
	public void setWorkDescription(String workDescription) {
		this.workDescription = workDescription;
	}
	
	
	
}
