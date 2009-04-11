package net.notdot.bdbdatastore.server;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.entity.Entity.EntityProto;
import com.google.appengine.entity.Entity.Path;
import com.google.appengine.entity.Entity.Reference;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class AppDatastore {
	final Logger logger = LoggerFactory.getLogger(AppDatastore.class);
	
	protected String app_id;
	protected Environment env;
	protected Database entities;
	protected Map<Reference, Sequence> sequences = new HashMap<Reference, Sequence>();
	
	public AppDatastore(String basedir, String app_id)
			throws EnvironmentLockedException, DatabaseException {
		this.app_id = app_id;
		
		File datastore_dir = new File(basedir, app_id);
		datastore_dir.mkdir();
		
		EnvironmentConfig envconfig = new EnvironmentConfig();
		envconfig.setAllowCreate(true);
		envconfig.setTransactional(true);
		envconfig.setSharedCache(true);
		env = new Environment(datastore_dir, envconfig);
		
		DatabaseConfig dbconfig = new DatabaseConfig();
		dbconfig.setAllowCreate(true);
		dbconfig.setTransactional(true);
		entities = env.openDatabase(null, "entities", dbconfig);
		
		env.openDatabase(null, "blahtest", dbconfig);
	}
	
	public void close() throws DatabaseException {
		entities.close();
		env.close();
	}
	
	public EntityProto get(Reference ref, Transaction tx) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(ref.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus status = entities.get(tx, key, value, null);
		if(status == OperationStatus.SUCCESS) {
			try {
				return EntityProto.parseFrom(value.getData());
			} catch(InvalidProtocolBufferException ex) {
				logger.error("Invalid protocol buffer encountered parsing {}", ref);
			}
		}
		return null;
	}
	
	protected long getId(Reference ref) throws DatabaseException {
		Sequence seq = this.sequences.get(ref);
		if(seq == null) {
			synchronized(this.sequences) {
				seq = this.sequences.get(ref);
				if(seq == null) {
					SequenceConfig conf = new SequenceConfig();
					conf.setAllowCreate(true);
					conf.setCacheSize(DatastoreServer.properties.getInt("datastore.sequence.cache_size", 20));
					conf.setInitialValue(1);
					seq = entities.openSequence(null, new DatabaseEntry(ref.toByteArray()), conf);
					this.sequences.put(ref, seq);
				}
			}
		}
		return seq.get(null, 1);
	}
	
	public Reference put(EntityProto entity, Transaction tx) throws DatabaseException {
		Reference ref = entity.getKey();
		int pathLen = ref.getPath().getElementCount();
		Path.Element lastElement = ref.getPath().getElement(pathLen - 1);
		if(lastElement.getId() == 0 && !lastElement.hasName()) {
			long id = this.getId(ref);
			ref = Reference.newBuilder(ref).setPath(
					Path.newBuilder(ref.getPath())
					.setElement(pathLen - 1, 
							Path.Element.newBuilder(lastElement).setId(id))).build();
			entity = EntityProto.newBuilder(entity).setKey(ref).build();
		}
		
		DatabaseEntry key = new DatabaseEntry(ref.toByteArray());
		DatabaseEntry value = new DatabaseEntry(entity.toByteArray());
		OperationStatus status = entities.put(tx, key, value);
		if(status != OperationStatus.SUCCESS)
			throw new DatabaseException(String.format("Failed to put entity %s: put returned %s", entity.getKey(), status));
		return ref;
	}

	public Transaction newTransaction() throws DatabaseException {
		TransactionConfig conf = new TransactionConfig();
		return this.env.beginTransaction(null, conf);
	}

	public void delete(Reference ref, Transaction tx) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(ref.toByteArray());
		OperationStatus status = entities.delete(tx, key);
		if(status != OperationStatus.SUCCESS && status != OperationStatus.NOTFOUND) {
			throw new DatabaseException(String.format("Failed to delete entity %s: delete returned %s", ref, status));
		}
	}
}
