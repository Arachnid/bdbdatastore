package net.notdot.bdbdatastore.server;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.protorpc.RpcFailedError;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.sleepycat.je.DatabaseException;

public class Datastore {
	final Logger logger = LoggerFactory.getLogger(Datastore.class);

	protected String basedir;
	protected Map<String, AppDatastore> datastores = new HashMap<String, AppDatastore>();
		
	public Datastore(String basedir) {
		this.basedir = basedir;
	}
	
	public AppDatastore getAppDatastore(String app_id) {
		AppDatastore ret = datastores.get(app_id);
		if(ret != null)
			return ret;
		synchronized(datastores) {
			ret = datastores.get(app_id);
			if(ret != null)
				return ret;
			try {
				ret = new AppDatastore(basedir, app_id);
				datastores.put(app_id, ret);
				return ret;
			} catch(DatabaseException ex) {
				logger.error(String.format("Could not open datastore for app '%s'", app_id), ex);
				throw new RpcFailedError(String.format("Unable to get datastore instance for app '%s'", app_id),
						DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
			}
		}
	}
	
	public void close() {
		synchronized(datastores) {
			for(Map.Entry<String,AppDatastore> entry : datastores.entrySet()) {
				try {
					entry.getValue().close();
				} catch(DatabaseException ex) {
					logger.error(String.format("Error shutting down datastore for app '%s'", entry.getKey()), ex);
				}
			}
		}
	}
}
