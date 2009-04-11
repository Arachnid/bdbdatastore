package net.notdot.bdbdatastore.server;

import com.google.protobuf.Service;

import net.notdot.protorpc.ServiceFactory;

public class DatastoreServiceFactory extends ServiceFactory {
	protected Datastore datastore;
	
	public DatastoreServiceFactory(Datastore ds) {
		this.datastore = ds;
	}

	@Override
	public Service getService() {
		return new DatastoreService(this.datastore);
	}
}
