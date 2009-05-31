package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.Property;
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
	private ByteString[] fields;
	
	public CompositeIndexIndexer(Entity.Index idx) {
		this.kind = idx.getEntityType();
		this.hasAncestor = idx.getAncestor();
		
		// Store the list of field names
		this.fields = new ByteString[idx.getPropertyCount()];
		for(int i = 0; i < idx.getPropertyCount(); i++)
			this.fields[i] = idx.getProperty(i).getName();
	}

	public void createSecondaryKeys(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, Set<DatabaseEntry> results)
			throws DatabaseException {
		Entity.EntityProto entity;
		try {
			entity = Indexing.EntityData.parseFrom(data.getData()).getData();
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
		
		// Get a sorted list of properties
		List<Entity.Property> properties = new ArrayList<Entity.Property>(entity.getPropertyList());
		Collections.sort(properties, PropertyComparator.instance);
		
		// Add pseudo-properties
		properties.add(Entity.Property.newBuilder()
				.setName(QuerySpec.KEY_PROPERTY)
				.setValue(EntityKeyComparator.toPropertyValue(entity.getKey()))
				.build());
		
		// Construct a map of field name to first occurrence
		Map<ByteString, Integer> fieldMap = new HashMap<ByteString, Integer>(properties.size());
		ByteString currentName = null;
		for(int i = 0; i < properties.size(); i++) {
			Entity.Property currentProperty = properties.get(i);
			if(!currentProperty.getName().equals(currentName)) {
				currentName = currentProperty.getName();
				fieldMap.put(currentName, i);
			}
		}
		
		if(this.hasAncestor) {
			this.generateAncestorEntries(results, entity, properties, fieldMap);
		} else {
			this.generateEntries(results, properties, fieldMap, Indexing.CompositeIndexKey.newBuilder(), 0);
		}
	}

	private void generateEntries(Set<DatabaseEntry> results, List<Property> properties,
			Map<ByteString, Integer> fieldMap, Indexing.CompositeIndexKey.Builder current, int idx) {
		ByteString field = this.fields[idx];
		if(!fieldMap.containsKey(field))
			// Entity does not contain all fields from index.
			return;
		
		int initialOffset = fieldMap.get(field);
		
		// Step through the list of properties until we find one
		// with a different name to the one we're handling.
		for(int i = initialOffset; i < properties.size(); i++) {
			Property currentProperty = properties.get(i);
			if(!currentProperty.getName().equals(field))
				break;
			
			// Ensure any recursive invocations pick the next element in the list
			fieldMap.put(field, i + 1);
			
			// Recurse
			Indexing.CompositeIndexKey.Builder newEntry = current.clone().addValue(currentProperty.getValue());
			if(idx + 1 == this.fields.length) {
				results.add(new DatabaseEntry(newEntry.build().toByteArray()));
			} else {
				generateEntries(results, properties, fieldMap, newEntry, idx + 1);
			}
		}
		
		// Restore the original value for this element of the fieldMap
		fieldMap.put(field, initialOffset);
	}

	private void generateAncestorEntries(Set<DatabaseEntry> results, Entity.EntityProto entity,
			List<Property> properties, Map<ByteString, Integer> fieldMap) {
		Entity.Path.Builder ancestor = Entity.Path.newBuilder();
		
		List<Entity.Path.Element> elements = entity.getKey().getPath().getElementList();
		for(int i = 0; i < elements.size() - 1; i++) {
			ancestor.addElement(elements.get(i));
			Indexing.CompositeIndexKey.Builder newEntry = Indexing.CompositeIndexKey.newBuilder();
			newEntry.setAncestor(ancestor.clone());
			if(this.fields.length == 0) {
				// Ancestor-only index
				results.add(new DatabaseEntry(newEntry.build().toByteArray()));
			} else {
				generateEntries(results, properties, fieldMap, newEntry, 0);
			}
		}
	}
}
