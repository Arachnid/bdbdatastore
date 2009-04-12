package net.notdot.bdbdatastore.server;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.datastore_v3.DatastoreV3.Query;
import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.EntityProto;
import com.google.appengine.entity.Entity.Path;
import com.google.appengine.entity.Entity.Reference;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class AppDatastore {
	final Logger logger = LoggerFactory.getLogger(AppDatastore.class);
	
	protected String app_id;
	
	// The database environment, containing all the tables and indexes.
	protected Environment env;
	
	// The primary entities table. The primary key is the encoded Reference protocol buffer.
	// References are sorted first by kind, then by path, so we can also use this to satisfy
	// kind and ancestor queries.
	protected Database entities;
	
	// This table stores counter values. We can't store them in the entities table, because getSequence
	// inserts records in the database it's called on.
	protected Database sequences;
	
	// We define a single built-in index for satisfying equality queries on fields.
	protected SecondaryDatabase entities_by_property;
	
	// Cached sequences
	protected Map<Reference, Sequence> sequence_cache = new HashMap<Reference, Sequence>();
	
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
		dbconfig.setBtreeComparator(SerializedReferenceComparator.class);
		entities = env.openDatabase(null, "entities", dbconfig);

		sequences = env.openDatabase(null, "sequences", dbconfig);
	}
	
	public void close() throws DatabaseException {
		for(Sequence seq : this.sequence_cache.values())
			seq.close();
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
		Sequence seq = this.sequence_cache.get(ref);
		if(seq == null) {
			synchronized(this.sequence_cache) {
				seq = this.sequence_cache.get(ref);
				if(seq == null) {
					SequenceConfig conf = new SequenceConfig();
					conf.setAllowCreate(true);
					conf.setCacheSize(DatastoreServer.properties.getInt("datastore.sequence.cache_size", 20));
					conf.setInitialValue(1);
					seq = entities.openSequence(null, new DatabaseEntry(ref.toByteArray()), conf);
					this.sequence_cache.put(ref, seq);
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

	public DatastoreResultSet executeQuery(Query request) throws DatabaseException {
		DatastoreResultSet ret = getEntityQueryPlan(request);
		if(ret != null)
			return ret;
		//TODO: Handle running out of query plans
		return null;
	}

	/* Attempts to generate a query plan for a scan by entity only */
	private DatastoreResultSet getEntityQueryPlan(Query request) throws DatabaseException {
		//TODO: Explicitly handle __key__ sort order specification.
		if(request.hasAncestor() || request.getFilterCount() > 0 || request.getOrderCount() > 0)
			return null;
		
		Cursor cursor = this.entities.openCursor(null, null);
		// Create a key with just app and kind set.
		Entity.Reference startKey = Entity.Reference.newBuilder()
				.setApp(request.getApp())
				.setPath(Entity.Path.newBuilder()
						.addElement(Entity.Path.Element.newBuilder()
								.setType(request.getKind()))).build();
		return new DatastoreResultSet(cursor, startKey, request);
	}
}
