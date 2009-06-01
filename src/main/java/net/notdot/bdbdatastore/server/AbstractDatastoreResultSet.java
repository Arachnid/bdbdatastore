package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.notdot.bdbdatastore.Indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.entity.Entity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

public abstract class AbstractDatastoreResultSet {
	static final Logger logger = LoggerFactory.getLogger(DatastorePKResultSet.class);
	
	private int remaining = 0;
	private Set<Indexing.EntityKey> seen = new HashSet<Indexing.EntityKey>();
	private boolean initialized = false;
	
	protected AppDatastore ds;
	protected QuerySpec query;
	protected DatabaseEntry currentValue = null;
	protected DatabaseEntry currentPKey = new DatabaseEntry();
	protected Indexing.EntityKey currentPKeyEnt = null;
	
	public AbstractDatastoreResultSet(AppDatastore ds, QuerySpec query) throws DatabaseException {
		this.ds = ds;
		this.query = query;
		if(query != null)
			this.remaining = query.getOffset() + query.getLimit();
	}
	
	// Opens the internal cursor for reading, positioning it for the next result
	protected abstract void openCursor() throws DatabaseException;
	
	// Closes the internal cursor
	protected abstract void closeCursor() throws DatabaseException;
	
	// Reads the next record. Must set currentValue to the new record's value; may set
	// currentKey to the new record's key.
	protected abstract boolean readInternal() throws DatabaseException, InvalidProtocolBufferException;

	protected boolean read() throws DatabaseException {
		if(!initialized) {
			initialized = true;
			this.skip(this.query.getOffset());
		}
		if(remaining == 0)
			return false;
		while(true) {
			try {
				currentPKeyEnt = null;
				if(!this.readInternal()) {
					remaining = 0;
					return false;
				}
				if(currentPKeyEnt == null)
					currentPKeyEnt = Indexing.EntityKey.parseFrom(currentPKey.getData());
				if(!seen.contains(currentPKeyEnt)) {
					seen.add(currentPKeyEnt);
					remaining--;
					return true;
				}
			} catch (InvalidProtocolBufferException e) {
				//TODO: Make this message more helpful somehow.
				logger.error("Invalid protocol buffer encountered");
			}
		}
	}
	
	protected void skip(int count) throws DatabaseException {
		for(int i = 0; i < count; i++) {
			if(!this.read())
				break;
		}
	}

	public boolean hasMore() {
		return remaining != 0;
	}

	public List<Entity.EntityProto> getNext(int count) throws DatabaseException {
		List<Entity.EntityProto> entities = new ArrayList<Entity.EntityProto>(count);

		for(int i = 0; i < count; i++) {
			if(this.read()) {
				assert this.currentValue != null;
				try {
					Entity.EntityProto entity = Indexing.EntityData.parseFrom(currentValue.getData()).getData();
					entities.add(entity);
				} catch(InvalidProtocolBufferException ex) {
					//TODO: Make this message more helpful somehow.
					logger.error("Invalid protocol buffer encountered");
				}
			}
		}
		
		return entities;
	}
}