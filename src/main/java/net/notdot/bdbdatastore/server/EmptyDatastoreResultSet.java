package net.notdot.bdbdatastore.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.DatabaseException;

public class EmptyDatastoreResultSet extends AbstractDatastoreResultSet {

	public EmptyDatastoreResultSet(AppDatastore ds, QuerySpec query) throws DatabaseException {
		super(ds, query);
	}

	@Override
	protected void closeCursor() throws DatabaseException { }

	@Override
	protected void openCursor() throws DatabaseException { }

	@Override
	protected boolean readInternal() throws DatabaseException,
			InvalidProtocolBufferException {
		return false;
	}

}
