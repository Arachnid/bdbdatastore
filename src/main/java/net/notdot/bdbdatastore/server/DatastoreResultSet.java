package net.notdot.bdbdatastore.server;



import java.util.Arrays;

import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

public class DatastoreResultSet extends AbstractDatastoreResultSet {
	protected Cursor cursor;
	protected Message startKey;
	protected boolean exclusiveMin;
	protected MessagePredicate predicate;
	
	public DatastoreResultSet(Cursor cur, Message startKey, boolean exclusiveMin, QuerySpec query, MessagePredicate predicate) throws DatabaseException {
		super(query);
		this.cursor = cur;
		this.startKey = startKey;
		this.exclusiveMin = exclusiveMin;
		this.predicate = predicate;

		if(query.getOffset() > 0)
			this.skip(query.getOffset());
	}
	
	protected boolean getNextInternal(DatabaseEntry key, DatabaseEntry data) throws DatabaseException {
		if(remaining == 0)
			return false;

		OperationStatus status;
		if(started) {
			status = cursor.getNext(key, data, null);
		} else {
			byte[] startKeyBytes = this.startKey.toByteArray();
			key.setData(startKeyBytes);
			status = cursor.getSearchKeyRange(key, data, null);
			if(status == OperationStatus.SUCCESS && exclusiveMin && Arrays.equals(startKeyBytes, key.getData())) {
				// First key and the minimum is exclusive - fetch the next one
				status = cursor.getNextNoDup(key, data, null);
			}
		}

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
				if(this.predicate != null) {
					// Deserialize the key
					Message keyent = this.startKey.newBuilderForType().mergeFrom(key.getData()).build();
					if(!this.predicate.evaluate(keyent))
						return null;
				}
				return Indexing.EntityData.parseFrom(value.getData()).getData();
			} catch(InvalidProtocolBufferException ex) {
				//TODO: Make this message more helpful somehow.
				logger.error("Invalid protocol buffer encountered");
			}
		}
	}

	public void close() throws DatabaseException {
		this.cursor.close();
	}
}
