package net.notdot.bdbdatastore.server;

import java.util.Properties;

public class TypedProperties extends Properties {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2000398306176947566L;

	public int getInt(String key, int default_value) {
		String strvalue = this.getProperty(key);
		if(strvalue != null) {
			return Integer.parseInt(strvalue);
		} else {
			return default_value;
		}
	}
}
