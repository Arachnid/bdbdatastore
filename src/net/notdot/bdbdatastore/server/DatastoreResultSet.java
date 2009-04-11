package net.notdot.bdbdatastore.server;

import com.google.appengine.datastore_v3.DatastoreV3.Query;
import com.google.protobuf.Message;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

public class DatastoreResultSet {
	protected Cursor cursor;
	protected Message startKey;
	protected Query query;
	protected boolean started = false;
	protected int remaining = -1;
	
	public DatastoreResultSet(Cursor cur, Message startKey, Query query) throws DatabaseException {
		this.cursor = cur;
		this.startKey = startKey;
		this.query = query;
		
		if(query.hasLimit())
			this.remaining = query.getLimit();
		
		// Skip any offset records
		if(query.hasOffset())
			this.skip(query.getOffset());
	}
	
	private void skip(int count) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		for(int i = 0; i < count; i++) {
			if(!this.getNextInternal(key, data))
				break;
		}
	}

	protected boolean getNextInternal(DatabaseEntry key, DatabaseEntry data) throws DatabaseException {
		if(remaining == 0)
			return false;
		
		OperationStatus status;
		if(started) {
			status = cursor.getNext(key, data, null);
		} else {
			key.setData(this.startKey.toByteArray());
			status = cursor.getSearchKeyRange(key, data, null);
		}
		
		if(status == OperationStatus.SUCCESS) {
			this.remaining--;
			return true;
		} else if(status == OperationStatus.NOTFOUND) {
			return false;
		} else {
			throw new DatabaseException(String.format("Failed to advance query cursor: returned %s", status));
		}
	}
}
