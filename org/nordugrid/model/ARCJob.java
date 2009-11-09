package org.nordugrid.model;

import java.util.List;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import org.apache.log4j.Logger;

public class ARCJob {

	private static Logger log = Logger.getLogger(ARCJob.class);

	private String globalId;

	private String globalOwner;

	private String jobName;

	private String cluster;

	private String queue;

	private String submissionTime;

	private String proxyExpirationTime;

	private String completionTime;

	private String stdout;

	private String stderr;

	private String stdin;

	private int cpuCount = -1;

	private int queueRank = -1;

	private String comment;

	private int exitCode;

	private String errors;

	private String status;

	private void parseAttributes(Attributes job) {
		globalId = setValue("nordugrid-job-globalid", job, "");
		globalOwner = setValue("nordugrid-job-globalowner", job, "");
		jobName = setValue("nordugrid-job-jobname", job, "");
		cluster = setValue("nordugrid-job-execcluster", job, "");
		queue = setValue("nordugrid-job-execqueue", job, "");
		comment = setValue("nordugrid-job-comment", job, "");
		completionTime = setValue("nordugrid-job-completiontime", job, "");
		cpuCount = setValue("nordugrid-job-cpucount", job, 1);
		errors = setValue("nordugrid-job-errors", job, "");
		exitCode = setValue("nordugrid-job-exitcode", job, 1);
		proxyExpirationTime = setValue("nordugrid-job-proxyexpirationtime", job, "");
		queueRank = setValue("nordugrid-job-queuerank", job, 1);
		status = setValue("nordugrid-job-status", job, "");
		stderr = setValue("nordugrid-job-stderr", job, "");
		stdin = setValue("nordugrid-job-stdin", job, "");
		stdout = setValue("nordugrid-job-stdout", job, "");
	}
	
	public ARCJob(Attributes job) {
		log.debug("Job found!");
		parseAttributes(job);
	}

	private String setValue(String name, Attributes attrs, String value) {
		try {
			String v = (String) (attrs.get(name).get());
			if (v != null) {
				value = v;
			}

		}
		catch (Exception err) {
		}
		return value;
	}

	private int setValue(String name, Attributes attrs, int value) {
		try {
			String v = setValue(name, attrs, null);
			value = Integer.parseInt(v);
		}
		catch (Exception err) {
		}
		return value;
	}

	private void setValues(String name, Attributes attrs, List values) {
		try {
			Attribute attr = attrs.get(name);
			for (int i = 0; i < attr.size(); i++) {
				String v = (String) (attrs.get(name).get(i));
				if (v == null)
					continue;
				int n = 0;
				for (; n < values.size(); n++) {
					if (v.equals((String) (values.get(n))))
						break;
				}
				if (n >= values.size()) {
					values.add(v);
				}
			}
		}
		catch (Exception err) {
		}
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getCompletionTime() {
		return completionTime;
	}

	public void setCompletionTime(String completionTime) {
		this.completionTime = completionTime;
	}

	public String getErrors() {
		return errors;
	}

	public void setErrors(String errors) {
		this.errors = errors;
	}

	public int getExitCode() {
		return exitCode;
	}

	public void setExitCode(int exitCode) {
		this.exitCode = exitCode;
	}

	public String getGlobalId() {
		return globalId;
	}

	public void setGlobalId(String globalId) {
		this.globalId = globalId;
	}

	public String getGlobalOwner() {
		return globalOwner;
	}

	public void setGlobalOwner(String globalOwner) {
		this.globalOwner = globalOwner;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getProxyExpirationTime() {
		return proxyExpirationTime;
	}

	public void setProxyExpirationTime(String proxyExpirationTime) {
		this.proxyExpirationTime = proxyExpirationTime;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getStderr() {
		return stderr;
	}

	public void setStderr(String stderr) {
		this.stderr = stderr;
	}

	public String getStdin() {
		return stdin;
	}

	public int getCpuCount() {
		return cpuCount;
	}

	public void setCpuCount(int cpuCount) {
		this.cpuCount = cpuCount;
	}

	public int getQueueRank() {
		return queueRank;
	}

	public void setQueueRank(int queueRank) {
		this.queueRank = queueRank;
	}

	public void setStdin(String stdin) {
		this.stdin = stdin;
	}

	public String getStdout() {
		return stdout;
	}

	public void setStdout(String stdout) {
		this.stdout = stdout;
	}

	public String getSubmissionTime() {
		return submissionTime;
	}

	public void setSubmissionTime(String submissionTime) {
		this.submissionTime = submissionTime;
	}

	public String toString() {
		return "Job with id " + this.globalId;
	}
}
