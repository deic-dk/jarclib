package tests;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.globus.rsl.AbstractRslNode;
import org.globus.rsl.NameOpValue;
import org.globus.rsl.RslNode;
import org.globus.rsl.Value;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.matcher.Matcher;
import org.nordugrid.matcher.SimpleMatcher;
import org.nordugrid.model.ARCResource;

public class ARCMatchersTest extends TestCase {

	private static Logger log = Logger.getLogger(ARCMatchersTest.class);
	static {
		Logger.getRootLogger().setLevel(Level.DEBUG);
	}
	public void testSimpleMatcher() throws ARCGridFTPJobException {
		Matcher matcher = new SimpleMatcher();
		
		// create pseudo-resource
		ARCResource res = new ARCResource();
		res.setArchitecture("Linux");
		res.setMiddleware("arc");
		res.setDiskspace(2000);
		res.setNodememory(2048);
		res.setMinCpuTime(10);
		res.setMaxCpuTime(23);
		Set access = new HashSet(2);
		access.add("inbound");

		access.add("outbound");
		res.setNodeAccess(access);

		// create pseudo-job
		RslNode root = new RslNode(AbstractRslNode.MULTI);
		RslNode child1 = new RslNode(AbstractRslNode.AND);
		RslNode child2 = new RslNode(AbstractRslNode.OR);
		RslNode child3 = new RslNode(AbstractRslNode.AND);
		child1.add(new NameOpValue("nodeAccess", NameOpValue.EQ, "inbound"));
		child1.add(new NameOpValue("middleware", NameOpValue.EQ, "arc"));
		child1.add(new NameOpValue("memory", NameOpValue.GTEQ, new Value("100")));
		child1.add(new NameOpValue("cpuTime", NameOpValue.EQ, new Value("20")));
		child2.add(new NameOpValue("architecture", NameOpValue.EQ, new Value("Linux")));
		child2.add(new NameOpValue("environment", NameOpValue.EQ, new Value("bar")));
		child2.add(new NameOpValue("disk", NameOpValue.GTEQ, new Value("100")));
		child2.add(new NameOpValue("memory", NameOpValue.GTEQ, new Value("100000")));
		child3.add(new NameOpValue("architecture", NameOpValue.EQ, new Value("MS")));
		
		log.setLevel(Level.ALL);
		root.add(child1);
		root.add(child2);
		log.debug(root.toRSL(true));
		assertTrue("Failed to qualify resource1 as suitable", matcher.isResourceSuitable(root.toRSL(true), res));
		root.add(child3);
		log.debug(root.toRSL(true));
		assertTrue("Failed to qualify resource as not suitable", !matcher.isResourceSuitable(root.toRSL(true), res));
		
		assertTrue("Failed to qualify resource2 as suitable", matcher.isResourceSuitable(child1.toRSL(true), res));	
	}

}
