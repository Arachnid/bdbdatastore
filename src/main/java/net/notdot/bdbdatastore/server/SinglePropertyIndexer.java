package net.notdot.bdbdatastore.server;

import java.util.Set;

import net.notdot.bdbdatastore.Indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;

public class SinglePropertyIndexer implements SecondaryMultiKeyCreator {
	static final Logger logger = LoggerFactory.getLogger(SinglePropertyIndexer.class);

	public void createSecondaryKeys(SecondaryDatabase db, DatabaseEntry key,
			DatabaseEntry data, Set<DatabaseEntry> results)
			throws DatabaseException {
		try {
			Entity.EntityProto entity = Indexing.EntityData.parseFrom(data.getData()).getData();
			Entity.Path path = entity.getKey().getPath();
			ByteString kind = path.getElement(path.getElementCount() - 1).getType();
			for(Entity.Property prop : entity.getPropertyList()) {
				results.add(new DatabaseEntry(Indexing.PropertyIndexKey.newBuilder()
					.setKind(kind)
					.setName(prop.getName())
					.setValue(prop.getValue()).build().toByteArray()));
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO: Make this error message more useful somehow.
			logger.error("Attempted to index invalid entity");
		}
	}
}
