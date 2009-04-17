package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.entity.Entity;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

public abstract class AbstractDatastoreResultSet {
	static final Logger logger = LoggerFactory.getLogger(DatastoreResultSet.class);
	
	protected QuerySpec query;
	
	protected boolean started = false;
	protected int remaining = -1;

	public AbstractDatastoreResultSet(QuerySpec query) throws DatabaseException {
		this.query = query;
		
		this.remaining = query.getLimit();
	}

	protected void skip(int count) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		for(int i = 0; i < count; i++) {
			if(!this.getNextInternal(key, data))
				break;
		}
	}

	protected abstract boolean getNextInternal(DatabaseEntry key, DatabaseEntry data) throws DatabaseException;

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

	protected abstract Entity.EntityProto getNext() throws DatabaseException;
	
	public abstract void close() throws DatabaseException;
}