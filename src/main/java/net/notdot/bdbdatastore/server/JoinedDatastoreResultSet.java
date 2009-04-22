package net.notdot.bdbdatastore.server;



import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.OperationStatus;

public class JoinedDatastoreResultSet extends AbstractDatastoreResultSet {
	protected JoinCursor cursor;
	protected Cursor[] gc = null;
	
	public JoinedDatastoreResultSet(JoinCursor cur, QuerySpec query, Cursor[] gc) throws DatabaseException {
		super(query);
		this.cursor = cur;
		this.gc = gc;

		if(query.getOffset() > 0)
			this.skip(query.getOffset());
	}
	
	protected boolean getNextInternal(DatabaseEntry key, DatabaseEntry data) throws DatabaseException {
		if(remaining == 0)
			return false;
		
		OperationStatus status = cursor.getNext(key, data, null);
		if(status == OperationStatus.SUCCESS) {
			this.remaining--;
			this.started = true;
			return true;
		} else if(status == OperationStatus.NOTFOUND) {
			return false;
		} else {
			throw new DatabaseException(String.format("Failed to advance query cursor: returned %s", status));
		}
	}

	protected Entity.EntityProto getNext() throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		
		while(true) {
			if(!this.getNextInternal(key, value))
				return null;
			
			try {
				return Indexing.EntityData.parseFrom(value.getData()).getData();
			} catch(InvalidProtocolBufferException ex) {
				//TODO: Make this message more helpful somehow.
				logger.error("Invalid protocol buffer encountered");
			}
		}
	}

	public void close() throws DatabaseException {
		this.cursor.close();
		if(this.gc != null) {
			for(Cursor cur : this.gc)
				cur.close();
		}
	}
}
