package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.entity.Entity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

public class DatastoreResultSet {
	static final Logger logger = LoggerFactory.getLogger(DatastoreResultSet.class);

	protected Cursor cursor;
	protected Message startKey;
	protected boolean exclusiveMin;
	protected QuerySpec query;
	protected MessagePredicate predicate;
	
	protected boolean started = false;
	protected int remaining = -1;
	protected List<FilterSpec> filters;

	public DatastoreResultSet(Cursor cur, Message startKey, boolean exclusiveMin, QuerySpec query, MessagePredicate predicate) throws DatabaseException {
		this.cursor = cur;
		this.startKey = startKey;
		this.exclusiveMin = exclusiveMin;
		this.query = query;
		this.predicate = predicate;
		
		this.remaining = query.getLimit();
		if(query.getOffset() > 0)
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
	
	public boolean hasMore() {
		return remaining != 0;
	}
	
	public List<Entity.EntityProto> getNext(int count) throws DatabaseException {
		List<Entity.EntityProto> entities = new ArrayList<Entity.EntityProto>(count);
		
		for(int i = 0; i < count; i++) {
			Entity.EntityProto ent = this.getNext();
			if(ent == null) {
				remaining = 0;
				break;
			}
			entities.add(ent);
		}
		
		return entities;
	}
	
	protected Entity.EntityProto getNext() throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		
		while(true) {
			if(!this.getNextInternal(key, value))
				return null;
			
			try {
				// Deserialize the key
				Message keyent = this.startKey.newBuilderForType().mergeFrom(key.getData()).build();
				if(!this.predicate.evaluate(keyent))
					return null;
				return Entity.EntityProto.parseFrom(value.getData());
			} catch(InvalidProtocolBufferException ex) {
				//TODO: Make this message more helpful somehow.
				logger.error("Invalid protocol buffer encountered");
			}
		}
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

	public void close() throws DatabaseException {
		this.cursor.close();
	}
}
