package org.nordugrid.matcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.globus.rsl.AbstractRslNode;
import org.globus.rsl.NameOpValue;
import org.globus.rsl.ParseException;
import org.globus.rsl.RSLParser;
import org.globus.rsl.RslNode;
import org.globus.rsl.Value;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.model.ARCResource;
import org.nordugrid.utils.XRSLAttributesEnumeration;

/**
 * Implementation of Matcher that provides on simple exact matching on some
 * resources.
 * 
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class SimpleMatcher implements Matcher {
	private static Logger log = Logger.getLogger(SimpleMatcher.class);

	public boolean isResourceSuitable(String xrsl, ARCResource resource) throws ARCGridFTPJobException {
		try {
			RslNode root = RSLParser.parse(xrsl);
			return isResourceSuitable(root, resource);
		}
		catch (ParseException e) {
			if (log.isDebugEnabled()) {
				log.debug("Failed to parse xRSL description");
				e.printStackTrace();
			}
			throw new ARCGridFTPJobException("Bad xRSL desription! " + e.getMessage());
		}

	}

	public boolean isResourceSuitable(RslNode xrsl, ARCResource resource) throws ARCGridFTPJobException {
		log.debug("Working with '" + xrsl.toRSL(true) + "'");
		// first of all, if the current job contains sub-specification, traverse
		// them
		if (xrsl.getOperator() == AbstractRslNode.MULTI) {
			if (xrsl.getSpecifications() == null)
				throw new ARCGridFTPJobException("Invalid job description!");
			for (Iterator it = xrsl.getSpecifications().iterator(); it.hasNext();) {
				boolean result = isSpecificationSuitable((RslNode) it.next(), resource);
				if (result == false)
					return false;
			}
		}
		else {
			return isSpecificationSuitable(xrsl, resource);
		}
		return true;
	}

	private boolean isSpecificationSuitable(RslNode spec, ARCResource resource) throws ARCGridFTPJobException {
		switch (spec.getOperator()) {
		case AbstractRslNode.MULTI:
			throw new ARCGridFTPJobException("Multi job operator (+) is only allowed at the top level!");
		case AbstractRslNode.OR:
		case AbstractRslNode.AND:
			boolean result = true;
			if (spec.getRelations() != null)
				result = isRelationMapSuitable(spec.getRelations(), resource, spec.getOperator());
			if (result == false)
				return result;
			if (spec.getSpecifications() != null) {
				for (Iterator it = spec.getSpecifications().iterator(); it.hasNext();) {
					result = isSpecificationSuitable((RslNode) it.next(), resource);
				}
			}
			return result;
		default:
			return false; // this should never happen however
		}
	}

	private static void printMapDebug(Map map) {
		for (Iterator it = map.keySet().iterator(); it.hasNext();) {
			Object o = it.next();
			log.debug("key = " + o + ", value = " + map.get(o));
		}
	}

	private static boolean isRelationMapSuitable(Map relations, ARCResource resource, int logic) throws ARCGridFTPJobException {

		boolean result = true;
		// check architecture
		String architecture = resource.getArchitecture();
		NameOpValue arcRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.ARCHITECTURE);
		if (arcRequested != null) {
			boolean tmp = ((Value) (arcRequested.getFirstValue())).getValue().equals(architecture);
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}

		if (result == false)
			return result;

		// check RTE
		Set rte = resource.getRuntimeenvironment();
		NameOpValue rteRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.RUNTIME_ENVIRONMENT);
		if (rteRequested != null) {
			List vals = new ArrayList();
			for (Iterator it = rteRequested.getValues().iterator(); it.hasNext();) {
				vals.add(((Value) it.next()).getValue());
			}
			boolean tmp = rte.containsAll(vals);
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}

		if (result == false)
			return result;

		// check queue
		String queue = resource.getQueueName();
		NameOpValue queueRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.QUEUE);
		if (queueRequested != null) {
			boolean tmp = ((Value) (queueRequested.getFirstValue())).getValue().equals(queue);
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}

		if (result == false)
			return result;

		// check running time
		int minTime = resource.getMinCpuTime();
		int maxTime = resource.getMaxCpuTime();
		NameOpValue timeRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.CPU_TIME);
		if (timeRequested != null) {
			int requestedTime = Integer.parseInt(((Value) (timeRequested.getFirstValue())).getValue());
			boolean tmp = requestedTime > minTime && requestedTime < maxTime;
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}
		
		if (result == false)
			return result;
		
		
		// check disk
		int disk = resource.getDiskspace();
		NameOpValue diskRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.DISK);
		if (diskRequested != null) {
			boolean tmp = true;
			switch (diskRequested.getOperator()) {
			case NameOpValue.EQ:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) == disk;
				break;
			case NameOpValue.GT:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) < disk;
				break;
			case NameOpValue.GTEQ:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) <= disk;
				break;
			case NameOpValue.LT:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) > disk;
				break;
			case NameOpValue.LTEQ:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) >= disk;
				break;
			case NameOpValue.NEQ:
				tmp = Integer.parseInt(diskRequested.getFirstValue().toString()) != disk;
				break;
			}
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}

		if (result == false)
			return result;

		// check memory
		int memory = resource.getNodememory();
		NameOpValue memRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.MEMORY);
		if (memRequested != null) {
			boolean tmp = true;
			switch (memRequested.getOperator()) {
			case NameOpValue.EQ:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) == memory;
				break;
			case NameOpValue.GT:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) < memory;
				break;
			case NameOpValue.GTEQ:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) <= memory;
				break;
			case NameOpValue.LT:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) > memory;
				break;
			case NameOpValue.LTEQ:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) >= memory;
				break;
			case NameOpValue.NEQ:
				result = Integer.parseInt(memRequested.getFirstValue().toString()) != memory;
				break;
			}
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}
		if (result == false)
			return result;

		// check node access
		Set nodeAccess = resource.getNodeAccess();
		NameOpValue nodeRequested = (NameOpValue) relations.remove(XRSLAttributesEnumeration.NODE_ACCESS);
		if (nodeRequested != null) {
			List vals = new ArrayList();
			for (Iterator it = nodeRequested.getValues().iterator(); it.hasNext();) {
				vals.add(((Value) it.next()).getValue());
			}
			boolean tmp = nodeAccess.containsAll(vals);
			result = logic == AbstractRslNode.AND ? result && tmp : result || tmp;
		}
		if (result == false)
			return result;

		// first remove all the other user-side attributes from the relations
		for (Iterator it = XRSLAttributesEnumeration.USER_SIDE_ATTRIBUTES.iterator(); it.hasNext();) {
			relations.remove(it.next());
		}

		// in case there are some strange attributes remaining - crash
		if (relations.size() != 0) {
			StringBuffer unknown = new StringBuffer();
			for (Iterator it = relations.keySet().iterator(); it.hasNext();) {
				unknown.append(it.next().toString());
				unknown.append(" ");
			}
			log.debug("Unknown xRSL attributes: " + unknown);
			throw new ARCGridFTPJobException("Unknown xRSL attributes: " + unknown);
		}

		return result; // should be true
	}

}
