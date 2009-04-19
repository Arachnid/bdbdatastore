package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
	protected Set<Entity.Reference> seen = new HashSet<Entity.Reference>();

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
		
		while(count > 0) {
			Entity.EntityProto ent = this.getNext();
			if(ent == null) {
				remaining = 0;
				break;
			}
			if(!this.seen.contains(ent.getKey())) {
				entities.add(ent);
				this.seen.add(ent.getKey());
				count--;
			}
		}
		
		return entities;
	}

	protected abstract Entity.EntityProto getNext() throws DatabaseException;
	
	public abstract void close() throws DatabaseException;
}