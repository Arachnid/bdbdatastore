package net.notdot.bdbdatastore.server;



import java.util.Arrays;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.OperationStatus;

public class JoinedDatastoreResultSet extends AbstractDatastoreResultSet {
	protected JoinCursor joinCursor = null;
	protected Cursor[] cursors = null;
	protected byte[][] startKeys = null;
	protected boolean positioned;
	
	public JoinedDatastoreResultSet(AppDatastore ds, QuerySpec query, byte[][] startKeys) throws DatabaseException {
		super(ds, query);
		this.startKeys = startKeys;
	}

	@Override
	protected void closeCursor() throws DatabaseException {
		// joinCursor will not be set, if the first equality filter is not found
		if (this.joinCursor == null) return;
		this.joinCursor.close();
		this.joinCursor = null;
		for(Cursor cur : this.cursors)
			 cur.close();
		this.cursors = null;
	}

	@Override
	protected void openCursor() throws DatabaseException {
		positioned = false;
	}

	@Override
	protected boolean readInternal() throws DatabaseException {
		OperationStatus status;
		if(!positioned) {
			cursors = new Cursor[startKeys.length];
			for(int i = 0; i < startKeys.length; i++) {
				cursors[i] = ds.entities_by_property.openCursor(null, ds.getCursorConfig());
				
				// Find the requested entry
				DatabaseEntry key = new DatabaseEntry(startKeys[i]);
				if(this.currentValue != null) {
					// Reopening the cursor
					DatabaseEntry value = new DatabaseEntry(currentValue.getData());
					status = cursors[i].getSearchBothRange(key, value, null);
					if(status == OperationStatus.SUCCESS) {
						if(Arrays.equals(startKeys[i], key.getData())) {
							// Skip the last record we read last time
							status = cursors[i].getNextDup(key, value, null);
						} else {
							// We reopened a cursor, but couldn't find anything.
							status = OperationStatus.NOTFOUND;
						}
					}
				} else {
					// First use
					DatabaseEntry value = new DatabaseEntry();
					status = cursors[i].getSearchKey(key, value, null);
				}
				// If we can't find it, the whole query returns 0 results
				if(status != OperationStatus.SUCCESS) {
					// The finally close should catch this
					// Close any cursors we already opened
					//for(int j = 0; j <= i; j++) 
						//cursors[i].close();
					return false;
				}
			}
			
			// Construct a join cursor
			joinCursor = ds.entities.join(cursors, null);
			
			positioned = true;
		}

		currentValue = new DatabaseEntry();
		status = joinCursor.getNext(currentPKey, currentValue, null);
		if(status == OperationStatus.SUCCESS) {
			return true;
		} else if(status == OperationStatus.NOTFOUND) {
			return false;
		} else {
			throw new DatabaseException(String.format("Failed to advance query cursor: returned %s", status));
		}
			
	}
}
