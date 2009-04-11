package net.notdot.bdbdatastore.server;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.protorpc.ProtoRpcController;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.sleepycat.je.DatabaseException;

public class Datastore {
	final Logger logger = LoggerFactory.getLogger(Datastore.class);

	protected String basedir;
	protected Map<String, AppDatastore> datastores = new HashMap<String, AppDatastore>();
		
	public Datastore(String basedir) {
		this.basedir = basedir;
	}
	
	public AppDatastore getAppDatastore(ProtoRpcController controller, String app_id) {
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
				logger.error("Could not open datastore for app {}: {}", app_id, ex);
				controller.setFailed(String.format("Unable to get datastore instance for app '%s'", app_id));
				controller.setApplicationError(DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
				return null;
			}
		}
	}
	
	public void close() {
		synchronized(datastores) {
			for(Map.Entry<String,AppDatastore> entry : datastores.entrySet()) {
				try {
					entry.getValue().close();
				} catch(DatabaseException ex) {
					logger.error("Error shutting down datastore for app '{}': {}", entry.getKey(), ex);
				}
			}
		}
	}
}
