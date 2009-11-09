package org.nordugrid.utils;

import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration of RSL attributes used in ARC middleware.
 * 
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class XRSLAttributesEnumeration {

	public static final String EXECUTABLE = "executable";

	public static final String ARGUMENTS = "arguments";

	public static final String INPUT_FILES = "inputfiles";

	public static final String EXECUTABLES = "executables";

	public static final String CACHE = "cache";

	public static final String OUTPUT_FILES = "outputfiles";

	public static final String GRID_TIME = "gridtime";

	public static final String MEMORY = "memory";

	public static final String DISK = "disk";

	public static final String RUNTIME_ENVIRONMENT = "runtimeenvironment";

	public static final String MIDDLEWARE = "middleware";

	public static final String STDOUT = "stdout";

	public static final String STDIN = "stdin";

	public static final String STDERR = "stderr";

	public static final String JOIN = "join";

	public static final String GMLOG = "gmlog";

	public static final String JOB_NAME = "jobname";

	public static final String FTP_THREADS = "ftpthreads";

	public static final String ACL = "acl";

	public static final String CLUSTER = "cluster";

	public static final String QUEUE = "queue";

	public static final String START_TIME = "starttime";

	public static final String LIFE_TIME = "lifetime";

	public static final String NOTIFY = "notify";

	public static final String NODE_ACCESS = "nodeaccess";

	public static final String ARCHITECTURE = "architecture";

	public static final String REPLICA_COLLECTION = "replicaCollection";

	public static final String RERUN = "rerun";

	public static final String DRY_RUN = "dryrun";

	public static final String RSL_SUBSTITUITION = "rsl_substituition";

	public static final String ENVIRONMENT = "environment";

	public static final String COUNT = "count";

	public static final String BENCHMARK = "benchmark";

	public static final String CPU_TIME = "cputime";

	public static final String JOB_REPORT = "jobreport";
	
	// Added by Frederik Orellana
	public static final String ACTION = "action";

	public static Set USER_SIDE_ATTRIBUTES = new HashSet();
	static {
		USER_SIDE_ATTRIBUTES.add(EXECUTABLE);
		USER_SIDE_ATTRIBUTES.add(ARGUMENTS);
		USER_SIDE_ATTRIBUTES.add(INPUT_FILES);
		USER_SIDE_ATTRIBUTES.add(CACHE);
		USER_SIDE_ATTRIBUTES.add(OUTPUT_FILES);
		USER_SIDE_ATTRIBUTES.add(GRID_TIME);
		USER_SIDE_ATTRIBUTES.add(MEMORY);
		USER_SIDE_ATTRIBUTES.add(DISK);
		USER_SIDE_ATTRIBUTES.add(RUNTIME_ENVIRONMENT);
		USER_SIDE_ATTRIBUTES.add(MIDDLEWARE);
		USER_SIDE_ATTRIBUTES.add(STDOUT);
		USER_SIDE_ATTRIBUTES.add(STDIN);
		USER_SIDE_ATTRIBUTES.add(STDERR);
		USER_SIDE_ATTRIBUTES.add(JOIN);
		USER_SIDE_ATTRIBUTES.add(JOB_NAME);
		USER_SIDE_ATTRIBUTES.add(FTP_THREADS);
		USER_SIDE_ATTRIBUTES.add(START_TIME);
		USER_SIDE_ATTRIBUTES.add(QUEUE);
		USER_SIDE_ATTRIBUTES.add(ARCHITECTURE);
		USER_SIDE_ATTRIBUTES.add(NODE_ACCESS);
		USER_SIDE_ATTRIBUTES.add(RERUN);
		USER_SIDE_ATTRIBUTES.add(DRY_RUN);
		USER_SIDE_ATTRIBUTES.add(BENCHMARK);
		USER_SIDE_ATTRIBUTES.add(COUNT);
		USER_SIDE_ATTRIBUTES.add(RSL_SUBSTITUITION);
		USER_SIDE_ATTRIBUTES.add(ACL);
		USER_SIDE_ATTRIBUTES.add(LIFE_TIME);
		USER_SIDE_ATTRIBUTES.add(REPLICA_COLLECTION);
		USER_SIDE_ATTRIBUTES.add(GMLOG);
		USER_SIDE_ATTRIBUTES.add(CLUSTER);
		USER_SIDE_ATTRIBUTES.add(ENVIRONMENT);
		USER_SIDE_ATTRIBUTES.add(CPU_TIME);
		USER_SIDE_ATTRIBUTES.add(JOB_REPORT);
		USER_SIDE_ATTRIBUTES.add(NOTIFY);
		// Added by Frederik Orellana
   		USER_SIDE_ATTRIBUTES.add(EXECUTABLES);
    	USER_SIDE_ATTRIBUTES.add(ACTION);		
	}
	
	public static boolean isValidUserSideAttribute(String attribute) {
		attribute = attribute.toLowerCase();
		// check only user-side attributes
		return USER_SIDE_ATTRIBUTES.contains(attribute);
	}
}
