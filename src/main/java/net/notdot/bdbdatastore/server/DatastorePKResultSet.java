package net.notdot.bdbdatastore.server;



import java.util.Arrays;

import net.notdot.bdbdatastore.Indexing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

public class DatastorePKResultSet extends AbstractDatastoreResultSet {
	protected Cursor cursor = null;
	protected boolean exclusiveMin;
	protected boolean positioned;
	protected MessagePredicate predicate;
	
	public DatastorePKResultSet(AppDatastore ds, Indexing.EntityKey startKey, boolean exclusiveMin, QuerySpec query, MessagePredicate predicate) throws DatabaseException {
		super(ds, query);
		this.currentPKey = new DatabaseEntry(startKey.toByteArray());
		this.exclusiveMin = exclusiveMin;
		this.predicate = predicate;
	}
	
	@Override
	protected void closeCursor() throws DatabaseException {
		this.cursor.close();
	}

	@Override
	protected void openCursor() throws DatabaseException {
		this.cursor = ds.entities.openCursor(null, ds.getCursorConfig());
		this.positioned = false;
	}

	@Override
	protected boolean readInternal() throws DatabaseException, InvalidProtocolBufferException {
		OperationStatus status;
		
		if(positioned) {
			// Cursor has already been used; just advance
			status = cursor.getNext(currentPKey, currentValue, null); 
		} else {
			if(currentValue == null) {
				// Cursor has never been used before
				this.currentValue = new DatabaseEntry();
				byte[] startKeyBytes = currentPKey.getData();
				status = cursor.getSearchKeyRange(currentPKey, currentValue, null);
				if(status == OperationStatus.SUCCESS && exclusiveMin && Arrays.equals(startKeyBytes, currentPKey.getData())) {
					// First key and the minimum is exclusive - fetch the next one
					status = cursor.getNextNoDup(currentPKey, currentValue, null);
				}
			} else {
				// Cursor has been reopened
				status = cursor.getSearchBothRange(currentPKey, currentValue, null);
				if(status == OperationStatus.SUCCESS) {
					status = cursor.getNext(currentPKey, currentValue, null);
				}
			}
			positioned = true;
		}
		if(status == OperationStatus.SUCCESS) {
			if(this.predicate != null) {
				this.currentPKeyEnt = Indexing.EntityKey.parseFrom(currentPKey.getData());
				return this.predicate.evaluate(this.currentPKeyEnt);
			}
			return true;
		} else if(status == OperationStatus.NOTFOUND) {
			return false;
		} else {
			throw new DatabaseException(String.format("Failed to advance query cursor: returned %s", status));
		}
	}
}
