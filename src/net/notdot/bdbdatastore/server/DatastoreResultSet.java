package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;
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
	protected DatastoreV3.Query query;
	protected boolean started = false;
	protected int remaining = -1;
	protected Map<ByteString, DatastoreV3.Query.Filter> filters = new HashMap<ByteString, DatastoreV3.Query.Filter>();

	public DatastoreResultSet(Cursor cur, Message startKey, DatastoreV3.Query query) throws DatabaseException {
		this.cursor = cur;
		this.startKey = startKey;
		this.query = query;
		
		// Index the filter properties
		for(DatastoreV3.Query.Filter filter : query.getFilterList())
			for(Entity.Property prop : filter.getPropertyList())
				filters.put(prop.getName(), filter);
		
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
	
	public boolean hasMore() {
		return remaining != 0;
	}
	
	public List<Entity.EntityProto> getNext(int count) throws DatabaseException {
		List<Entity.EntityProto> entities = new ArrayList<Entity.EntityProto>(count);
		
		for(int i = 0; i < count; i++) {
			Entity.EntityProto ent = this.getNext();
			if(ent == null)
				break;
			entities.add(ent);
		}
		
		return entities;
	}
	
	public Entity.EntityProto getNext() throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		Set<ByteString> seen = new HashSet<ByteString>();
		Entity.EntityProto entity = null;
		
		while(entity == null) {
			if(!this.getNextInternal(key, value))
				return null;
			
			try {
				// Deserialize the entity
				entity = Entity.EntityProto.parseFrom(value.getData());
			} catch(InvalidProtocolBufferException ex) {
				//TODO: Make this message more helpful somehow.
				logger.error("Invalid EntityProto protocol buffer encountered");
			}
		}
		
		Entity.Reference ref = entity.getKey();
		
		Entity.Path.Element lastElement = ref.getPath().getElement(ref.getPath().getElementCount() - 1);
		// Check kind
		if(!lastElement.getType().equals(this.query.getKind())) {
			remaining = 0;
			return null;
		}
		
		// Check app (should always match)
		if(!ref.getApp().equals(this.query.getApp())) {
			logger.error("Entity with app_id {} encountered when processing records for app_id {}", ref.getApp(), this.query.getApp());
			remaining = 0;
			return null;
		}
		
		return entity;
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
			started = true;
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

	public void close() throws DatabaseException {
		this.cursor.close();
	}
}
