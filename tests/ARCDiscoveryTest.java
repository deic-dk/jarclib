package tests;

import java.util.Collection;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.nordugrid.is.ARCDiscovery;
import org.nordugrid.is.ARCDiscoveryException;

public class ARCDiscoveryTest extends TestCase  {
	private String dn = "/O=Grid/O=NorduGrid/OU=hep.lu.se/CN=Oxana Smirnova";
	private ARCDiscovery d;
	
	public void setUp() {
		d = new ARCDiscovery("ldap://index4.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
		d.addGIIS("ldap://index1.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
		d.addGIIS("ldap://index2.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
		d.addGIIS("ldap://index3.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");	
	}
	
	
	public void testDiscoverAll() throws ARCDiscoveryException {		
	  d.discoverAll();
	  assertTrue("Discovery has found no clusters. Impossible!", d.getClusters().size() != 0);
	  assertTrue("Discovery has found no SEs. Impossible!", d.getSEs().size() != 0);	  
	}
	
	
	public void testFindAuthorizedQueues() throws ARCDiscoveryException, InterruptedException {		
	  d.discoverAll();	  
	  Logger.getRootLogger().setLevel(Level.DEBUG);
	  Logger.getRootLogger().setAdditivity(true);
	  long start = System.currentTimeMillis();
	  long limit = 10000;
	  long offset = 2000; // some +- coefficient
	  Collection foundResources = d.findAuthorizedResources(dn, 20, limit);
	  long end = System.currentTimeMillis();
	  assertTrue("Failed to stay within time limit", (end - start) < limit + offset);
	  assertTrue("Failed to find authorized queues", foundResources.size() != 0);	  
	}
	
	public void testFindUserJobs() throws ARCDiscoveryException, InterruptedException {		
	  d.discoverAll();	  
	  long start = System.currentTimeMillis();
	  long limit = 10000;
	  long offset = 2000; // some +- coefficient
	  Collection foundResources = d.findUserJobs(dn, 20, limit);
	  long end = System.currentTimeMillis();
	  assertTrue("Failed to stay within time limit", (end - start) < limit + offset);
	  assertTrue("Failed to find authorized queues", foundResources.size() != 0);	  
	}	
}
