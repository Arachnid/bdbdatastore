package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.PropertyValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;

public class CompositeIndexIndexer implements SecondaryMultiKeyCreator {
	static final Logger logger = LoggerFactory.getLogger(SinglePropertyIndexer.class);

	private ByteString kind;
	private boolean hasAncestor;
	private Map<ByteString, Integer> fields = new HashMap<ByteString, Integer>();
	
	public CompositeIndexIndexer(Entity.Index idx) {
		this.kind = idx.getEntityType();
		this.hasAncestor = idx.getAncestor();
		
		for(int i = 0; i < idx.getPropertyCount(); i++)
			this.fields.put(idx.getProperty(i).getName(), new Integer(i));
	}

	@SuppressWarnings("unchecked")
	public void createSecondaryKeys(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, Set<DatabaseEntry> results)
			throws DatabaseException {
		Entity.EntityProto entity;
		try {
			entity = Entity.EntityProto.parseFrom(data.getData());
		} catch (InvalidProtocolBufferException e) {
			// TODO: Make this error message more useful somehow.
			logger.error("Attempted to index invalid entity");
			return;
		}

		// Check the kind
		Entity.Path path = entity.getKey().getPath();
		ByteString kind = path.getElement(path.getElementCount() - 1).getType();
		if(!kind.equals(this.kind))
			return;
		
		// Create a list for each property value
		List<Entity.PropertyValue>[] lists = new List[this.fields.size()];
		Indexing.CompositeIndexKey.Builder entry = Indexing.CompositeIndexKey.newBuilder();
		for(int i = 0; i < this.fields.size(); i++) {
			lists[i] = new ArrayList<Entity.PropertyValue>();
			entry.addValue(Entity.PropertyValue.getDefaultInstance());
		}

		// Populate the lists
		ByteString current = null;
		int current_idx = -1;
		for(Entity.Property value : entity.getPropertyList()) {
			if(!value.getName().equals(current)) {
				current = value.getName();
				Integer idx = this.fields.get(current);
				current_idx = (idx==null)?-1:idx;
			}
			if(current_idx != -1)
				lists[current_idx].add(value.getValue());
		}
		
		// Generate all the index entries
		int count;
		int max = DatastoreServer.properties.getInt("datastore.max_index_entries", 1000);
		if(this.hasAncestor) {
			count = this.generateAncestorEntries(path, results, entry, lists, max);
		} else {
			count = this.generateEntries(results, entry, lists, 0, max);
		}
		if(count >= max) {
			logger.warn("Truncating index entries for {} at {}", entity.getKey(), count);
		}
	}

	private int generateAncestorEntries(Entity.Path path, Set<DatabaseEntry> results,
			Indexing.CompositeIndexKey.Builder entry, List<PropertyValue>[] lists, int max) {
		int count = 0;
		Entity.Path.Builder subpath = Entity.Path.newBuilder();
		for(int i = 0; i < path.getElementCount(); i++) {
			subpath.addElement(path.getElement(i));
			entry.setAncestor(subpath.clone());
			count += generateEntries(results, entry, lists, 0, max - count);
			if(count >= max)
				break;
		}
		return count;
	}

	private int generateEntries(Set<DatabaseEntry> results, Indexing.CompositeIndexKey.Builder entry,
			List<PropertyValue>[] lists, int idx, int max) {
		int count = 0;
		for(PropertyValue value : lists[idx]) {
			entry.setValue(idx, value);
			if(idx == lists.length - 1) {
				results.add(new DatabaseEntry(entry.clone().build().toByteArray()));
				count++;
			} else {
				count += generateEntries(results, entry, lists, idx + 1, max - count);
			}
			if(count >= max)
				break;
		}
		return count;
	}
}
