package net.notdot.bdbdatastore.server;

import com.google.appengine.entity.Entity.EntityProto;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

public class EmptyDatastoreResultSet extends AbstractDatastoreResultSet {

	public EmptyDatastoreResultSet(QuerySpec query) throws DatabaseException {
		super(query);
	}
	
	@Override
	public void close() throws DatabaseException { }

	@Override
	protected EntityProto getNext() throws DatabaseException {
		return null;
	}

	@Override
	protected boolean getNextInternal(DatabaseEntry key, DatabaseEntry data)
			throws DatabaseException {
		return false;
	}

}
