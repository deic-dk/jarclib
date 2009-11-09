package org.nordugrid.utils;

import org.apache.log4j.Logger;
import org.globus.rsl.ParseException;
import org.globus.rsl.RSLParser;

/**
 * Common RSL utilities.
 * @author Ilja Livenson (ilja_l@tudeng.ut.ee)
 */
public class RSLUtilities {
	private static Logger log = Logger.getLogger(RSLUtilities.class);

	public static boolean isValidRsl(String desc) {
		try {
			RSLParser.parse(desc);
		}
		catch (ParseException e) {
			return false;
		}
		return true;
	}

}
