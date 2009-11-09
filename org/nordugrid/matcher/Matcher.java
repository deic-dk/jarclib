package org.nordugrid.matcher;

import org.globus.rsl.RslNode;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.model.ARCResource;

/**
 * Common interface for matching the job description against a resource.
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public interface Matcher {
	/**
	 * @return true if the resource and the specification match. False otherwise.
	 */
	 public boolean isResourceSuitable(RslNode xrsl, ARCResource resource) throws ARCGridFTPJobException;
	 
	 /**
		 * @return true if the resource and the specification match. False otherwise.
		 */
	 public boolean isResourceSuitable(String description, ARCResource resource) throws ARCGridFTPJobException;
}
