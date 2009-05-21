package net.notdot.bdbdatastore.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.notdot.bdbdatastore.Indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.base.ApiBase;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.datastore_v3.DatastoreV3.CompositeIndices;
import com.google.appengine.datastore_v3.DatastoreV3.Query;
import com.google.appengine.datastore_v3.DatastoreV3.Schema;
import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.CompositeIndex;
import com.google.appengine.entity.Entity.EntityProto;
import com.google.appengine.entity.Entity.Path;
import com.google.appengine.entity.Entity.Property;
import com.google.appengine.entity.Entity.Reference;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcCallback;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class AppDatastore {
	final Logger logger = LoggerFactory.getLogger(AppDatastore.class);
		
	protected String app_id;
	
	protected File datastore_dir;

	// The database environment, containing all the tables and indexes.
	protected Environment env;
	
	// The primary entities table. The primary key is the encoded Reference protocol buffer.
	// References are sorted first by kind, then by path, so we can also use this to satisfy
	// kind and ancestor queries.
	protected Database entities;
	
	// This table stores counter values. We can't store them in the entities table, because getSequence
	// inserts records in the database it's called on.
	protected Database sequences;

	// Cached sequences
	protected Map<Reference, Sequence> sequence_cache = new HashMap<Reference, Sequence>();
	
	// We define a single built-in index for satisfying equality queries on fields.
	protected SecondaryDatabase entities_by_property;
	
	// Maps index definitions to IDs
	protected Map<Entity.Index, Long> index_ids = new HashMap<Entity.Index, Long>();
	protected long next_index_id;
	
	// Maps index definitions to index databases
	protected Map<Entity.Index, SecondaryDatabase> indexes = new ConcurrentHashMap<Entity.Index, SecondaryDatabase>();
	
	/**
	 * @param basedir
	 * @param app_id
	 * @throws EnvironmentLockedException
	 * @throws DatabaseException
	 */
	public AppDatastore(String basedir, String app_id)
			throws EnvironmentLockedException, DatabaseException {
		logger.info("Initializing datastore for app '{}'...", app_id);
		
		this.app_id = app_id;
		
		datastore_dir = new File(basedir, app_id);
		datastore_dir.mkdir();
		
		EnvironmentConfig envconfig = new EnvironmentConfig();
		envconfig.setAllowCreate(true);
		envconfig.setTransactional(true);
		envconfig.setSharedCache(true);
		env = new Environment(datastore_dir, envconfig);
		
		logger.info("  {}: Opening entities table", app_id);
		DatabaseConfig dbconfig = new DatabaseConfig();
		dbconfig.setAllowCreate(true);
		dbconfig.setTransactional(true);
		dbconfig.setBtreeComparator(SerializedEntityKeyComparator.class);
		entities = env.openDatabase(null, "entities", dbconfig);
		
        logger.info("  {}: Opening sequences table", app_id);
		sequences = env.openDatabase(null, "sequences", dbconfig);

		logger.info("  {}: Opening entities_by_property index", app_id);
		SecondaryConfig secondconfig = new SecondaryConfig();
		secondconfig.setAllowCreate(true);
		secondconfig.setAllowPopulate(true);
		secondconfig.setBtreeComparator(SerializedPropertyIndexKeyComparator.class);
		secondconfig.setDuplicateComparator(SerializedEntityKeyComparator.class);
		secondconfig.setMultiKeyCreator(new SinglePropertyIndexer());
		secondconfig.setSortedDuplicates(true);
		secondconfig.setTransactional(true);
		entities_by_property = env.openSecondaryDatabase(null, "entities_by_property", entities, secondconfig);
		
		loadCompositeIndexes();
		
		logger.info("  {}: Datastore initialized.", app_id);
	}
	
	public void addIndex(Entity.CompositeIndex idx, RpcCallback<ApiBase.Integer64Proto> done) throws DatabaseException {
		Entity.Index idxDef = idx.getDefinition();
		
		synchronized(this.index_ids) {
			if(this.index_ids.containsKey(idxDef)) {
				if(done != null)
					done.run(ApiBase.Integer64Proto.newBuilder().setValue(this.index_ids.get(idxDef)).build());
				return;
			}
			if(idx.getId() == 0) {
				idx = Entity.CompositeIndex.newBuilder(idx).setId(this.next_index_id++).build();
			}
			this.index_ids.put(idxDef, idx.getId());
		}
		if(done != null)
			done.run(ApiBase.Integer64Proto.newBuilder().setValue(idx.getId()).build());
		
		logger.info("  {}: Loading composite index 'idx-{}'", this.app_id, idx.getId());
		logger.debug("  {}: Composite index definition: {}", this.app_id, idx);
		SecondaryConfig config = new SecondaryConfig();
		config.setAllowCreate(true);
		config.setAllowPopulate(true);
		config.setBtreeComparator(new SerializedCompositeIndexKeyComparator(idxDef));
		config.setDuplicateComparator(SerializedEntityKeyComparator.class);
		config.setMultiKeyCreator(new CompositeIndexIndexer(idxDef));
		config.setSortedDuplicates(true);
		config.setTransactional(true);
		
		String idxName = String.format("idx-%X", idx.getId());
		SecondaryDatabase idxDb = env.openSecondaryDatabase(null, idxName, entities, config);
		this.indexes.put(idxDef, idxDb);
	}
	
	private void loadCompositeIndexes() throws DatabaseException {
		this.index_ids.clear();
		this.indexes.clear();
		this.next_index_id = 1;
		
		InputStream idxdata = null;
		try {
			idxdata = new FileInputStream(new File(this.datastore_dir, "indexes.dat"));
			Indexing.IndexList indexList = Indexing.IndexList.parseFrom(idxdata);
			
			for(Entity.CompositeIndex idx : indexList.getIndexList())
				this.addIndex(idx, null);
			this.next_index_id = indexList.getNextId();
		} catch(FileNotFoundException ex) {
			// Do nothing - no custom indexes present.
		} catch (IOException e) {
			// Failed to read indexes
		} finally {
			try {
				if(idxdata != null)
					idxdata.close();
			} catch(IOException e) {
				// At least we tried.
			}
		}
		// TODO: Add code to find and delete stray index DBs
	}
	
	public void saveCompositeIndexes() throws IOException {
		Indexing.IndexList.Builder indexList = Indexing.IndexList.newBuilder();
		for(Map.Entry<Entity.Index, Long> item : this.index_ids.entrySet()) {
			indexList.addIndex(Entity.CompositeIndex.newBuilder()
				.setAppId(this.app_id)
				.setId(item.getValue())
				.setDefinition(item.getKey())
				.setState(Entity.CompositeIndex.State.READ_WRITE)
				.build());
		}
		indexList.setNextId(this.next_index_id);
		
		OutputStream idxout = new FileOutputStream(new File(this.datastore_dir, "indexes.dat"));
		indexList.build().writeTo(idxout);
		idxout.close();
	}
	
	public void close() throws DatabaseException {
		for(Sequence seq : this.sequence_cache.values())
			seq.close();
		sequences.close();
		entities_by_property.close();
		entities.close();
		env.close();
	}
	
	protected static Indexing.EntityKey toEntityKey(Reference ref) {
		Entity.Path path = ref.getPath();
		ByteString kind = path.getElement(path.getElementCount() - 1).getType();
		return Indexing.EntityKey.newBuilder().setKind(kind).setPath(path).build();
	}
	
	public EntityProto get(Reference ref, Transaction tx) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(toEntityKey(ref).toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus status = entities.get(tx, key, value, null);
		if(status == OperationStatus.SUCCESS) {
			try {
				return Indexing.EntityData.parseFrom(value.getData()).getData();
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
					seq = sequences.openSequence(null, new DatabaseEntry(toEntityKey(ref).toByteArray()), conf);
					this.sequence_cache.put(ref, seq);
				}
			}
		}
		return seq.get(null, 1);
	}
	
	public Reference put(EntityProto entity, Transaction tx) throws DatabaseException {
		// Stable-sort the properties by name only for easy filtering on retrieval.
		List<Property> properties = new ArrayList<Property>(entity.getPropertyList());
		Collections.sort(properties, PropertyComparator.noValueInstance);
		entity = Entity.EntityProto.newBuilder(entity).clearProperty().addAllProperty(properties).build();
		
		// Generate and set the ID if necessary.
		Reference ref = entity.getKey();
		int pathLen = ref.getPath().getElementCount();
		Path.Element lastElement = ref.getPath().getElement(pathLen - 1);
		if(lastElement.getId() == 0 && !lastElement.hasName()) {
			long id = this.getId(ref);
			ref = Reference.newBuilder(ref).setPath(
					Path.newBuilder(ref.getPath())
					.setElement(pathLen - 1, 
							Path.Element.newBuilder(lastElement).setId(id))).build();
			if(ref.getPath().getElementCount() == 1) {
				entity = EntityProto.newBuilder(entity).setEntityGroup(ref.getPath()).setKey(ref).build();
			} else {
				entity = EntityProto.newBuilder(entity).setKey(ref).build();
			}
		}
		
		DatabaseEntry key = new DatabaseEntry(toEntityKey(ref).toByteArray());
		DatabaseEntry value = new DatabaseEntry(Indexing.EntityData.newBuilder()
				.setData(entity).build().toByteArray());
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
		DatabaseEntry key = new DatabaseEntry(toEntityKey(ref).toByteArray());
		OperationStatus status = entities.delete(tx, key);
		if(status != OperationStatus.SUCCESS && status != OperationStatus.NOTFOUND) {
			throw new DatabaseException(String.format("Failed to delete entity %s: delete returned %s", ref, status));
		}
	}

	public AbstractDatastoreResultSet executeQuery(Query request) throws DatabaseException {
		QuerySpec query = new QuerySpec(request);
		
		AbstractDatastoreResultSet ret = getEntityQueryPlan(query);
		if(ret != null)
			return ret;
		ret = getAncestorQueryPlan(query);
		if(ret != null)
			return ret;
		ret = getSinglePropertyQueryPlan(query);
		if(ret != null)
			return ret;
		ret = getCompositeIndexPlan(query);
		if(ret != null)
			return ret;
		ret = getMergeJoinQueryPlan(query);
		if(ret != null)
			return ret;
		return null;
	}

	private AbstractDatastoreResultSet getCompositeIndexPlan(QuerySpec query) throws DatabaseException {
		Entity.Index idx = null;
		SecondaryDatabase idxDb = null;
		for(Map.Entry<Entity.Index, SecondaryDatabase> entry : this.indexes.entrySet()) {
			if(query.isValidIndex(entry.getKey())) {
				idx = entry.getKey();
				idxDb = entry.getValue();
			}
		}
		if(idxDb == null)
			return null;
		
		// Construct a start key
		List<Entity.PropertyValue> values = new ArrayList<Entity.PropertyValue>();
		boolean exclusiveMin = query.getBounds(idx, 1, values);
		Indexing.CompositeIndexKey.Builder lowerBound = Indexing.CompositeIndexKey.newBuilder()
			.addAllValue(values);
		values.clear();
		boolean exclusiveMax = query.getBounds(idx, -1, values);
		Indexing.CompositeIndexKey.Builder upperBound = Indexing.CompositeIndexKey.newBuilder()
			.addAllValue(values);
		
		if(query.hasAncestor()) {
			lowerBound.setAncestor(query.getAncestor().getPath());
			upperBound.setAncestor(query.getAncestor().getPath());
		}
		
		Cursor cursor = idxDb.openCursor(null, null);
		MessagePredicate predicate = new CompositeIndexPredicate(idx, upperBound.build(), exclusiveMax);
		return new DatastoreResultSet(cursor, lowerBound.build(), exclusiveMin, query, predicate);
	}

	/* Attempts to generate a merge join multiple-equality query. */
	private AbstractDatastoreResultSet getMergeJoinQueryPlan(QuerySpec query) throws DatabaseException {
		if(query.hasAncestor())
			return null;

		// Check only equality filters are used
		if(query.hasInequalities())
			return null;
		
		// Check no sort orders are specified
		// TODO: Handle explicit specification of __key__ sort order
		if(query.getOrders().size() > 0)
			return null;
		
		Entity.Index index = query.getIndex();
		
		// Upper bound is equal to lower bound, since there's no inequality filter
		List<Entity.PropertyValue> values = new ArrayList<Entity.PropertyValue>(index.getPropertyCount());
		query.getBounds(query.getIndex(), 1, values);
		if(values.size() != index.getPropertyCount())
			return null;
		
		// Construct the required cursors
		Cursor[] cursors = new Cursor[values.size()];
		for(int i = 0; i < values.size(); i++) {
			Indexing.PropertyIndexKey startKey = Indexing.PropertyIndexKey.newBuilder()
				.setKind(index.getEntityType())
				.setName(index.getProperty(i).getName())
				.setValue(values.get(i))
				.build();
			cursors[i] = this.entities_by_property.openCursor(null, null);
			
			// Find the requested entry
			DatabaseEntry key = new DatabaseEntry(startKey.toByteArray());
			DatabaseEntry data = new DatabaseEntry();
			OperationStatus status = cursors[i].getSearchKey(key, data, null);
			// If we can't find it, the whole query returns 0 results
			if(status != OperationStatus.SUCCESS) {
				// Close any cursors we already opened
				for(int j = 0; j <= i; j++) 
					cursors[i].close();
				return new EmptyDatastoreResultSet(query);
			}
		}
		
		// Construct a join cursor
		JoinCursor cursor = this.entities.join(cursors, null);
		
		return new JoinedDatastoreResultSet(cursor, query, cursors);
	}
	
	/* Attempts to generate a query on a single-property index. */
	private AbstractDatastoreResultSet getSinglePropertyQueryPlan(QuerySpec query) throws DatabaseException {
		if(query.hasAncestor())
			return null;
		
		Entity.Index index = query.getIndex();
		if(index.getPropertyCount() > 1)
			return null;
		// We don't do descending sort orders
		if(index.getPropertyCount() == 1 && index.getProperty(0).getDirection() != Entity.Index.Property.Direction.ASCENDING)
			return null;
		
		List<Entity.PropertyValue> values = new ArrayList<Entity.PropertyValue>(1);
		Indexing.PropertyIndexKey.Builder lowerBound = Indexing.PropertyIndexKey.newBuilder()
			.setKind(index.getEntityType())
			.setName(index.getProperty(0).getName());
		boolean exclusiveMin = query.getBounds(query.getIndex(), 1, values);
		if(values.size() == 1) {
			lowerBound.setValue(values.get(0));
		} else if(values.size() > 1) {
			return null;
		}
		
		Indexing.PropertyIndexKey.Builder upperBound = Indexing.PropertyIndexKey.newBuilder()
			.setKind(index.getEntityType())
			.setName(index.getProperty(0).getName());
		values.clear();
		boolean exclusiveMax = query.getBounds(query.getIndex(), -1, values);
		// Special case for equality queries: getBounds returns a sentinel value for the upper bound.
		if(values.size() == 1 || (values.size() == 2 && values.get(1).equals(Entity.PropertyValue.getDefaultInstance()))) {
			upperBound.setValue(values.get(0));
		} else if(values.size() > 1) {
			return null;
		}
		
		Cursor cursor = this.entities_by_property.openCursor(null, null);
		MessagePredicate predicate = new PropertyIndexPredicate(upperBound.build(), exclusiveMax);
		return new DatastoreResultSet(cursor, lowerBound.build(), exclusiveMin, query, predicate);
	}


	/* Attempts to generate a query by ancestor and entity */
	private AbstractDatastoreResultSet getAncestorQueryPlan(QuerySpec query) throws DatabaseException {
		if(!query.hasAncestor() || query.getOrders().size() > 0)
			return null;
		
		Indexing.EntityKey keyPrefix = Indexing.EntityKey.newBuilder()
			.setKind(query.getKind())
			.setPath(query.getAncestor().getPath())
			.build();
		return doPrimaryIndexPlan(query, keyPrefix);
	}

	/* Attempts to generate a query plan for a scan by entity only */
	private AbstractDatastoreResultSet getEntityQueryPlan(QuerySpec query) throws DatabaseException {
		if(query.hasAncestor() || query.getOrders().size() > 0)
			return null;
		
		Indexing.EntityKey keyPrefix = Indexing.EntityKey.newBuilder()
			.setKind(query.getKind())
			.build();
		return doPrimaryIndexPlan(query, keyPrefix);
	}
	
	private AbstractDatastoreResultSet doPrimaryIndexPlan(QuerySpec query, Indexing.EntityKey keyPrefix) throws DatabaseException {
		Indexing.EntityKey startKey;
		
		List<Entity.PropertyValue> values = new ArrayList<Entity.PropertyValue>(1);
		boolean lowerExclusive = false;
		boolean upperExclusive = false;
		if(query.getFilters().size() > 1)
			return null;
		if(query.getFilters().size() == 1 && !query.getFilters().containsKey(QuerySpec.KEY_PROPERTY))
			return null;
		
		if(query.getFilters().size() == 1) {
			lowerExclusive = query.getBounds(query.getIndex(), 1, values);
			if(values.size() > 1)
				return null;
		}
		
		Indexing.EntityKey lowerBound = null;
		if(values.size() == 1)
			lowerBound = EntityKeyComparator.toEntityKey(values.get(0).getReferenceValue());
		// If we have a __key__ query with a lower bound, and it's greater than the ancestor key...
		if(lowerBound != null && EntityKeyComparator.instance.compare(lowerBound, keyPrefix) > 0) {
			startKey = lowerBound;
		} else {
			startKey = keyPrefix;
		}
		
		values.clear();
		if(query.getFilters().size() == 1) {
			upperExclusive = query.getBounds(query.getIndex(), -1, values);
			if(values.size() > 1)
				return null;
		}
		
		MessagePredicate predicate = new KeyPredicate(keyPrefix);
		Indexing.EntityKey endKey = null;
		if(values.size() == 1 && values.get(0).getReferenceValue().getPathElementCount() > 0)
			endKey = EntityKeyComparator.toEntityKey(values.get(0).getReferenceValue());
		// If the query has an upper bound and it's before we would stop anyway...
		if(endKey != null && predicate.evaluate(endKey))
			predicate = new KeyRangePredicate(endKey, upperExclusive);
		
		Cursor cursor = this.entities.openCursor(null, null);
		return new DatastoreResultSet(cursor, startKey, lowerExclusive, query, predicate);
	}

	public boolean deleteIndex(CompositeIndex idx) throws DatabaseException {
		Entity.Index idxDef = idx.getDefinition();
		
		SecondaryDatabase idxDb;
		synchronized(this.index_ids) {
			Long index_id = this.index_ids.get(idxDef);
			if(index_id == null || index_id.longValue() != idx.getId())
				return false;
			idxDb = this.indexes.get(idxDef);
			if(idxDb == null)
				return false;
			this.index_ids.remove(idxDef);
			this.indexes.remove(idxDef);
		}
		
		// TODO: There's a potential synchronization issue here - what if the index is being queried when we delete it?
		idxDb.close();
		return true;
	}

	public CompositeIndices getIndices() {
		DatastoreV3.CompositeIndices.Builder response = DatastoreV3.CompositeIndices.newBuilder();
		synchronized(this.index_ids) {
			for(Map.Entry<Entity.Index, Long> entry : this.index_ids.entrySet()) {
				Entity.CompositeIndex.Builder index = Entity.CompositeIndex.newBuilder();
				index.setAppId(this.app_id);
				index.setId(entry.getValue());
				index.setDefinition(entry.getKey());
				if(this.indexes.containsKey(entry.getKey())) {
					index.setState(Entity.CompositeIndex.State.READ_WRITE);
				} else {
					index.setState(Entity.CompositeIndex.State.WRITE_ONLY);
				}
				response.addIndex(index);
			}
		}
		return response.build();
	}

	public Schema getSchema() throws DatabaseException {
		SecondaryCursor cursor = this.entities_by_property.openSecondaryCursor(null, null);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		DatastoreV3.Schema.Builder schema = DatastoreV3.Schema.newBuilder();
		Entity.EntityProto.Builder entity = null;
		ByteString currentEntityType = null;
		
		OperationStatus status = cursor.getFirst(key, data, null);
		while(status == OperationStatus.SUCCESS) {
			try {
				Indexing.PropertyIndexKey propertyKey = Indexing.PropertyIndexKey.parseFrom(key.getData());
				if(!propertyKey.getKind().equals(currentEntityType)) {
					if(entity != null)
						schema.addKind(entity);
					currentEntityType = propertyKey.getKind();
					entity = Entity.EntityProto.newBuilder()
						.setKey(Entity.Reference.newBuilder()
							.setApp(app_id)
							.setPath(Entity.Path.newBuilder()
								.addElement(Entity.Path.Element.newBuilder()
									.setType(currentEntityType))))
						.setEntityGroup(Entity.Path.getDefaultInstance());
				}
				entity.addProperty(Entity.Property.newBuilder()
					.setName(propertyKey.getName())
					.setValue(Entity.PropertyValue.getDefaultInstance())
					.setMultiple(false));

				// Assemble a key that's greater than this one
				byte[] newName = new byte[propertyKey.getName().size() + 1];
				propertyKey.getName().copyTo(newName, 0);
				key.setData(Indexing.PropertyIndexKey.newBuilder(propertyKey)
					.setName(ByteString.copyFrom(newName))
					.build().toByteArray());
				status = cursor.getSearchKeyRange(key, data, null);
			} catch(InvalidProtocolBufferException ex) {
				logger.error("Invalid protocol buffer encountered in getSchema");
				status = cursor.getNext(key, data, null);
			}
		}
		if(entity != null)
			schema.addKind(entity);

		return schema.build();
	}
}
