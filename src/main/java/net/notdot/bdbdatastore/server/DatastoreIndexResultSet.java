package net.notdot.bdbdatastore.server;



import java.util.Arrays;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

public class DatastoreIndexResultSet extends AbstractDatastoreResultSet {
	protected SecondaryCursor cursor = null;
	protected SecondaryDatabase db;
	protected boolean exclusiveMin;
	protected boolean positioned;
	protected MessagePredicate predicate;
	protected Message startKey;
	protected DatabaseEntry currentKey;
	
	public DatastoreIndexResultSet(AppDatastore ds, SecondaryDatabase db, Message startKey, boolean exclusiveMin, QuerySpec query, MessagePredicate predicate) throws DatabaseException {
		super(ds, query);
		this.db = db;
		this.startKey = startKey;
		this.currentKey = new DatabaseEntry(startKey.toByteArray());
		this.exclusiveMin = exclusiveMin;
		this.predicate = predicate;
	}
	
	@Override
	protected void closeCursor() throws DatabaseException {
		this.cursor.close();
	}

	@Override
	protected void openCursor() throws DatabaseException {
		this.cursor = db.openSecondaryCursor(null, ds.getCursorConfig());
		this.positioned = false;
	}

	@Override
	protected boolean readInternal() throws DatabaseException, InvalidProtocolBufferException {
		OperationStatus status;
		
		if(positioned) {
			// Cursor has already been used; just advance
			status = cursor.getNext(currentKey, currentPKey, currentValue, null);
		} else {
			if(currentValue == null) {
				// Cursor has never been used before
				this.currentValue = new DatabaseEntry();
				byte[] startKeyBytes = currentKey.getData();
				status = cursor.getSearchKeyRange(currentKey, currentPKey, currentValue, null);
				if(status == OperationStatus.SUCCESS && exclusiveMin && Arrays.equals(startKeyBytes, currentKey.getData())) {
					// First key and the minimum is exclusive - fetch the next one
					status = cursor.getNextNoDup(currentKey, currentPKey, currentValue, null);
				}
			} else {
				// Cursor has been reopened
				status = cursor.getSearchBothRange(currentKey, currentPKey, currentValue, null);
				if(status == OperationStatus.SUCCESS) {
					status = cursor.getNext(currentKey, currentPKey, currentValue, null);
				}
			}
			positioned = true;
		}
		if(status == OperationStatus.SUCCESS) {
			if(this.predicate != null) {
				Message keyent;
				keyent = this.startKey.newBuilderForType().mergeFrom(currentKey.getData()).build();
				return this.predicate.evaluate(keyent);
			}
			return true;
		} else if(status == OperationStatus.NOTFOUND) {
			return false;
		} else {
			throw new DatabaseException(String.format("Failed to advance query cursor: returned %s", status));
		}
	}
}
