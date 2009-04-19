package net.notdot.bdbdatastore.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.notdot.bdbdatastore.Indexing;
import net.notdot.protorpc.ProtoRpcController;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.base.ApiBase;
import com.google.appengine.base.ApiBase.VoidProto;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

public class DatastoreServiceTest {
	protected class TestRpcCallback<T> implements RpcCallback<T> {
		protected T value = null;
		protected boolean called = false;
		
		public T getValue() {
			return value;
		}

		public boolean isCalled() {
			return called;
		}

		public void run(T arg0) {
			value = arg0;
			called = true;
		}		
	}
	
	// Sample key
	protected static Entity.Reference testkey = Entity.Reference.newBuilder()
		.setApp("testapp").setPath(
				Entity.Path.newBuilder().addElement(
						Entity.Path.Element.newBuilder()
						.setType(ByteString.copyFromUtf8("testtype"))
						.setName(ByteString.copyFromUtf8("testname")))).build();
	
	// Sample entity
	protected static Entity.EntityProto testent = Entity.EntityProto.newBuilder()
		.setKey(testkey)
		.setEntityGroup(testkey.getPath())
		.addProperty(Entity.Property.newBuilder()
				.setName(ByteString.copyFromUtf8("bar"))
				.setValue(Entity.PropertyValue.newBuilder().setStringValue(ByteString.copyFromUtf8("Hello, world!"))))
	.addProperty(Entity.Property.newBuilder()
			.setName(ByteString.copyFromUtf8("foo"))
			.setValue(Entity.PropertyValue.newBuilder().setInt64Value(1234))).build();
	
	// Sample entity with no ID
	protected static Entity.EntityProto testnewent = Entity.EntityProto.newBuilder(testent)
		.setKey(Entity.Reference.newBuilder().setApp("testapp").setPath(
				Entity.Path.newBuilder().addElement(
						Entity.Path.Element.newBuilder().setType(ByteString.copyFromUtf8("testtype"))))).build();
	
	// Sample composite index
	Entity.CompositeIndex compositeIdx = Entity.CompositeIndex.newBuilder()
		.setAppId("testapp")
		.setId(0)
		.setState(Entity.CompositeIndex.State.READ_WRITE) // Ignored
		.setDefinition(Entity.Index.newBuilder()
			.setEntityType(ByteString.copyFromUtf8("wtype"))
			.setAncestor(false)
			.addProperty(Entity.Index.Property.newBuilder()
				.setName(ByteString.copyFromUtf8("tags"))
				.setDirection(Entity.Index.Property.Direction.ASCENDING))
			.addProperty(Entity.Index.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setDirection(Entity.Index.Property.Direction.DESCENDING))
		).build();
	
	Entity.CompositeIndex compositeAncestorIdx = Entity.CompositeIndex.newBuilder(compositeIdx)
		.setDefinition(Entity.Index.newBuilder(compositeIdx.getDefinition())
			.setAncestor(true))
		.build();
	
	protected File basedir;
	protected Datastore datastore;
	protected DatastoreService service;
	protected Entity.Reference sample_key = null;
	protected DatastoreV3.PutRequest dataset_put;
	
	@Before
	public void setUp() throws IOException {
		basedir = File.createTempFile("bdbdatastore", "tmp", new File(System.getProperty("java.io.tmpdir")));
		basedir.delete();
		basedir.mkdir();
		datastore = new Datastore(basedir.getAbsolutePath());
		service = new DatastoreService(datastore);

		DatastoreV3.PutRequest.Builder request = DatastoreV3.PutRequest.newBuilder();
		InputStream dataSet = ClassLoader.getSystemResourceAsStream("dataset.txt");
		BufferedReader reader = new BufferedReader ( new InputStreamReader ( dataSet ) );
		TextFormat.merge(reader, request);
		dataset_put = request.build();
	}
	
	@After
	public void cleanUp() {
		service.close();
		datastore.close();
		for(File f : basedir.listFiles())
			f.delete();
		basedir.delete();
	}
	
	@Test
	public void testBeginTransaction() {
		RpcController controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.Transaction> done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, VoidProto.getDefaultInstance(), done);
		assertTrue(done.isCalled());
		assertEquals(done.getValue().getHandle(), 0);
		assertTrue(service.transactions.containsKey(done.getValue()));
		assertEquals(service.transactions.get(done.getValue()), null);
		
		controller = new ProtoRpcController();
		done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, VoidProto.getDefaultInstance(), done);
		assertTrue(done.isCalled());
		assertEquals(done.getValue().getHandle(), 1);
		assertTrue(service.transactions.containsKey(done.getValue()));
		assertEquals(service.transactions.get(done.getValue()), null);
	}

	@Test
	public void testCommit() {
		RpcController controller = new ProtoRpcController();

		// Create a transaction
		TestRpcCallback<DatastoreV3.Transaction> tx_done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, ApiBase.VoidProto.getDefaultInstance(), tx_done);
		assertTrue(tx_done.isCalled());
		DatastoreV3.Transaction tx = tx_done.getValue();
		
		// Create an entity
		TestRpcCallback<DatastoreV3.PutResponse> put_done = new TestRpcCallback<DatastoreV3.PutResponse>();
		DatastoreV3.PutRequest put_request = DatastoreV3.PutRequest.newBuilder().addEntity(testent).setTransaction(tx).build();
		service.put(controller, put_request, put_done);
		assertTrue(put_done.isCalled());

		// Commit the transaction
		TestRpcCallback<ApiBase.VoidProto> void_done = new TestRpcCallback<ApiBase.VoidProto>();
		service.commit(controller, tx, void_done);
		assertTrue(void_done.isCalled());
		
		// Check the entity is there
		controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.GetResponse> get_done = new TestRpcCallback<DatastoreV3.GetResponse>();
		DatastoreV3.GetRequest get_request = DatastoreV3.GetRequest.newBuilder().addKey(testkey).build();
		service.get(controller, get_request, get_done);
		assertTrue(get_done.isCalled());
		assertEquals(get_done.getValue().getEntity(0).getEntity(), testent);
		
		// Ensure the transaction is gone
		assertFalse(service.transactions.containsKey(tx));
	}
	
	@Test
	public void testCommitEmpty() {
		RpcController controller = new ProtoRpcController();

		// Create a transaction
		TestRpcCallback<DatastoreV3.Transaction> tx_done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, ApiBase.VoidProto.getDefaultInstance(), tx_done);
		assertTrue(tx_done.isCalled());
		DatastoreV3.Transaction tx = tx_done.getValue();
		
		// Commit the transaction
		TestRpcCallback<ApiBase.VoidProto> void_done = new TestRpcCallback<ApiBase.VoidProto>();
		service.commit(controller, tx, void_done);
		assertTrue(void_done.isCalled());
	}

	@Test
	public void testDelete() {
		RpcController controller = new ProtoRpcController();
		
		// Create an entity
		TestRpcCallback<DatastoreV3.PutResponse> put_done = new TestRpcCallback<DatastoreV3.PutResponse>();
		DatastoreV3.PutRequest put_request = DatastoreV3.PutRequest.newBuilder().addEntity(testent).build();
		service.put(controller, put_request, put_done);
		assertTrue(put_done.isCalled());
		
		// Check it's there
		controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.GetResponse> get_done = new TestRpcCallback<DatastoreV3.GetResponse>();
		DatastoreV3.GetRequest get_request = DatastoreV3.GetRequest.newBuilder().addKey(testkey).build();
		service.get(controller, get_request, get_done);
		assertTrue(get_done.isCalled());
		assertEquals(get_done.getValue().getEntity(0).getEntity(), testent);
		
		// Delete it
		controller = new ProtoRpcController();
		TestRpcCallback<ApiBase.VoidProto> del_done = new TestRpcCallback<ApiBase.VoidProto>();
		DatastoreV3.DeleteRequest del_request = DatastoreV3.DeleteRequest.newBuilder().addKey(testkey).build();
		service.delete(controller, del_request, del_done);
		assertTrue(del_done.isCalled());
		
		// Check it's not there
		controller = new ProtoRpcController();
		get_done = new TestRpcCallback<DatastoreV3.GetResponse>();
		service.get(controller, get_request, get_done);
		assertTrue(get_done.isCalled());		
		assertFalse(get_done.getValue().getEntity(0).hasEntity());
	}

	@Test
	public void testGet() {
		this.testPut();
		
		RpcController controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.GetResponse> done = new TestRpcCallback<DatastoreV3.GetResponse>();
		// Get the two entities we put in testPut()
		DatastoreV3.GetRequest request = DatastoreV3.GetRequest.newBuilder().addKey(testkey).addKey(sample_key).build();
		service.get(controller, request, done);
		
		assertTrue(done.isCalled());
		assertEquals(done.getValue().getEntityCount(), 2);
		
		// Entity with key name was retrieved correctly
		assertEquals(done.getValue().getEntity(0).getEntity(), testent);
		
		// Entity with assigned ID was retrieved correctly
		assertEquals(done.getValue().getEntity(1).getEntity().getKey(), sample_key);
		assertEquals(done.getValue().getEntity(1).getEntity().getPropertyList(), testnewent.getPropertyList());
		assertEquals(done.getValue().getEntity(1).getEntity().getEntityGroup(), sample_key.getPath());
	}

	@Test
	public void testPut() {
		RpcController controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.PutResponse> done = new TestRpcCallback<DatastoreV3.PutResponse>();
		// Test putting an entity with a name, and one with neither name nor ID
		DatastoreV3.PutRequest request = DatastoreV3.PutRequest.newBuilder().addEntity(testent).addEntity(testnewent).build();
		service.put(controller, request, done);
		assertTrue(done.isCalled());
		assertEquals(done.getValue().getKeyCount(), 2);

		// Entity with name was inserted correctly
		assertEquals(done.getValue().getKey(0), testkey);
		
		// Entity with no id was inserted correctly and assigned an id
		sample_key = done.getValue().getKey(1);
		assertEquals(sample_key.getApp(), testkey.getApp());
		assertEquals(1, sample_key.getPath().getElementCount());
		assertEquals(testnewent.getKey().getPath().getElement(0).getType(), sample_key.getPath().getElement(0).getType());
		assertTrue(sample_key.getPath().getElement(0).hasId());
		assertTrue(sample_key.getPath().getElement(0).getId() > 0);
	}

	@Test
	public void testRollback() {
		RpcController controller = new ProtoRpcController();

		// Create a transaction
		TestRpcCallback<DatastoreV3.Transaction> tx_done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, ApiBase.VoidProto.getDefaultInstance(), tx_done);
		assertTrue(tx_done.isCalled());
		DatastoreV3.Transaction tx = tx_done.getValue();
		
		// Create an entity
		TestRpcCallback<DatastoreV3.PutResponse> put_done = new TestRpcCallback<DatastoreV3.PutResponse>();
		DatastoreV3.PutRequest put_request = DatastoreV3.PutRequest.newBuilder().addEntity(testent).setTransaction(tx).build();
		service.put(controller, put_request, put_done);
		assertTrue(put_done.isCalled());

		// Rollback the transaction
		TestRpcCallback<ApiBase.VoidProto> void_done = new TestRpcCallback<ApiBase.VoidProto>();
		service.rollback(controller, tx, void_done);
		assertTrue(void_done.isCalled());
		
		// Check the entity is not there
		controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.GetResponse> get_done = new TestRpcCallback<DatastoreV3.GetResponse>();
		DatastoreV3.GetRequest get_request = DatastoreV3.GetRequest.newBuilder().addKey(testkey).build();
		service.get(controller, get_request, get_done);
		assertTrue(get_done.isCalled());
		assertFalse(get_done.getValue().getEntity(0).hasEntity());
		
		// Ensure the transaction is gone
		assertFalse(service.transactions.containsKey(tx));
	}

	@Test
	public void testRollbackEmpty() {
		RpcController controller = new ProtoRpcController();

		// Create a transaction
		TestRpcCallback<DatastoreV3.Transaction> tx_done = new TestRpcCallback<DatastoreV3.Transaction>();
		service.beginTransaction(controller, ApiBase.VoidProto.getDefaultInstance(), tx_done);
		assertTrue(tx_done.isCalled());
		DatastoreV3.Transaction tx = tx_done.getValue();
		
		// Roll back the transaction
		TestRpcCallback<ApiBase.VoidProto> void_done = new TestRpcCallback<ApiBase.VoidProto>();
		service.rollback(controller, tx, void_done);
		assertTrue(void_done.isCalled());
	}
	
	protected void loadCorpus() throws ParseException, FileNotFoundException, IOException {
		// Insert the test corpus
		RpcController controller = new ProtoRpcController();
		TestRpcCallback<DatastoreV3.PutResponse> put_done = new TestRpcCallback<DatastoreV3.PutResponse>();
		service.put(controller, dataset_put, put_done);
		assertTrue(put_done.isCalled());
	}
	
	@Test
	public void testKeyComparer() {
		Indexing.EntityKey startKey = Indexing.EntityKey.newBuilder().setKind(ByteString.copyFromUtf8("testtype")).build();
		assertTrue(EntityKeyComparator.instance.compare(startKey, AppDatastore.toEntityKey(dataset_put.getEntity(0).getKey())) > 0);
		assertTrue(EntityKeyComparator.instance.compare(startKey, AppDatastore.toEntityKey(dataset_put.getEntity(1).getKey())) < 0);
		assertTrue(EntityKeyComparator.instance.compare(startKey, AppDatastore.toEntityKey(dataset_put.getEntity(2).getKey())) < 0);
		assertTrue(EntityKeyComparator.instance.compare(startKey, AppDatastore.toEntityKey(dataset_put.getEntity(3).getKey())) < 0);
	}
	
	@Test
	public void testEntityOrdering() throws ParseException, FileNotFoundException, IOException, DatabaseException {
		loadCorpus();
		AppDatastore ds = this.service.datastore.getAppDatastore("testapp");
		Cursor cur = ds.entities.openCursor(null, null);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		cur.getFirst(key, data, null);
		assertEquals(AppDatastore.toEntityKey(dataset_put.getEntity(0).getKey()), Indexing.EntityKey.parseFrom(key.getData()));
		assertEquals(dataset_put.getEntity(0), Entity.EntityProto.parseFrom(data.getData()));
		
		for(int i = 1; i < 4; i++) {
			cur.getNext(key, data, null);
			assertEquals(AppDatastore.toEntityKey(dataset_put.getEntity(i).getKey()), Indexing.EntityKey.parseFrom(key.getData()));
			assertEquals(dataset_put.getEntity(i), Entity.EntityProto.parseFrom(data.getData()));
		}
		
		cur.close();
	}
	
	@Test
	public void testEntityQuery() throws ParseException, FileNotFoundException, IOException {
		RpcController controller = new ProtoRpcController();

		loadCorpus();
		
		// Construct a query for a kind
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("testtype")).build();
		TestRpcCallback<DatastoreV3.QueryResult> query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, query_done);
		assertTrue(query_done.isCalled());
		assertTrue(service.cursors.containsKey(query_done.getValue().getCursor()));
		
		// Get the results
		controller = new ProtoRpcController();
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(query_done.getValue().getCursor())
			.setCount(5).build();
		query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, query_done);
		assertTrue(query_done.isCalled());
		
		assertEquals(2, query_done.getValue().getResultCount());
		assertEquals(dataset_put.getEntity(1), query_done.getValue().getResult(0));
		assertEquals(dataset_put.getEntity(2), query_done.getValue().getResult(1));
		
		// Delete the cursor
		controller = new ProtoRpcController();
		TestRpcCallback<ApiBase.VoidProto> delete_done = new TestRpcCallback<ApiBase.VoidProto>();
		service.deleteCursor(controller, query_done.getValue().getCursor(), delete_done);
		assertTrue(delete_done.isCalled());
		assertFalse(service.cursors.containsKey(query_done.getValue().getCursor()));
	}
	
	@Test
	public void testAncestorQuery() throws ParseException, FileNotFoundException, IOException {
		RpcController controller = new ProtoRpcController();

		loadCorpus();
		
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("vtype"))
			.setAncestor(Entity.Reference.newBuilder()
				.setApp("testapp")
				.setPath(Entity.Path.newBuilder()
					.addElement(Entity.Path.Element.newBuilder()
						.setType(ByteString.copyFromUtf8("vtype"))
						.setName(ByteString.copyFromUtf8("bar"))))).build();
		TestRpcCallback<DatastoreV3.QueryResult> query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, query_done);
		assertTrue(query_done.isCalled());
		assertTrue(service.cursors.containsKey(query_done.getValue().getCursor()));
		
		// Get the results
		controller = new ProtoRpcController();
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(query_done.getValue().getCursor())
			.setCount(5).build();
		query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, query_done);
		assertTrue(query_done.isCalled());
		
		assertEquals(3, query_done.getValue().getResultCount());
		assertEquals(dataset_put.getEntity(4), query_done.getValue().getResult(0));
		assertEquals(dataset_put.getEntity(5), query_done.getValue().getResult(1));
		assertEquals(dataset_put.getEntity(7), query_done.getValue().getResult(2));
	}
	
	@Test
	public void testSinglePropertyQuery() throws ParseException, FileNotFoundException, IOException {
		loadCorpus();
		
		String[] fields = new String[] {
			"tags",
			"num",
			"num",
			"num",
			"num"
		};
		int[] operators = new int[] {
			DatastoreV3.Query.Filter.Operator.EQUAL.getNumber(),
			DatastoreV3.Query.Filter.Operator.GREATER_THAN.getNumber(),
			DatastoreV3.Query.Filter.Operator.GREATER_THAN_OR_EQUAL.getNumber(),
			DatastoreV3.Query.Filter.Operator.LESS_THAN.getNumber(),
			DatastoreV3.Query.Filter.Operator.LESS_THAN_OR_EQUAL.getNumber()
		};
		Entity.PropertyValue[] values = new Entity.PropertyValue[] {
			Entity.PropertyValue.newBuilder().setStringValue(ByteString.copyFromUtf8("foo")).build(),
			Entity.PropertyValue.newBuilder().setInt64Value(3).build(),
			Entity.PropertyValue.newBuilder().setInt64Value(5).build(),
			Entity.PropertyValue.newBuilder().setInt64Value(10).build(),
			Entity.PropertyValue.newBuilder().setInt64Value(5).build()
		};
		String[][] keyNames = new String[][] {
			new String[] { "a", "b" },
			new String[] { "a", "d" },
			new String[] { "a", "d" },
			new String[] { "a", "b", "c" },
			new String[] { "a", "b", "c" }
		};
		
		for(int i = 0; i < operators.length; i++) {
			RpcController controller = new ProtoRpcController();
			DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
				.setApp("testapp")
				.setKind(ByteString.copyFromUtf8("wtype"))
				.addFilter(DatastoreV3.Query.Filter.newBuilder()
					.setOp(operators[i])
					.addProperty(Entity.Property.newBuilder()
						.setName(ByteString.copyFromUtf8(fields[i]))
						.setValue(values[i]))).build();
			TestRpcCallback<DatastoreV3.QueryResult> done = new TestRpcCallback<DatastoreV3.QueryResult>();
			service.runQuery(controller, query, done);
			assertTrue(done.isCalled());
			
			DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
				.setCursor(done.getValue().getCursor())
				.setCount(10).build();
			controller = new ProtoRpcController();
			done = new TestRpcCallback<DatastoreV3.QueryResult>();
			service.next(controller, next, done);
			assertTrue(done.isCalled());
			
			assertEquals(String.format("i=%d", i), keyNames[i].length, done.getValue().getResultCount());
			Set<String> keySet = new HashSet<String>(Arrays.asList(keyNames[i]));
			for(Entity.EntityProto entity : done.getValue().getResultList()) {
				String name = entity.getKey().getPath().getElement(0).getName().toStringUtf8();
				assertTrue(String.format("i=%d, key=%s", i, name), keySet.contains(name));
			}
		}
	}
	
	@Test
	public void testSinglePropertyRange() throws ParseException, FileNotFoundException, IOException {
		// Tests that a range query on a single property works.
		loadCorpus();
		
		RpcController controller = new ProtoRpcController();
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.GREATER_THAN.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setValue(Entity.PropertyValue.newBuilder().setInt64Value(3))))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.LESS_THAN.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setValue(Entity.PropertyValue.newBuilder().setInt64Value(10))))
			.addOrder(DatastoreV3.Query.Order.newBuilder().setProperty(ByteString.copyFromUtf8("num")))
			.build();
		TestRpcCallback<DatastoreV3.QueryResult> done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, done);
		assertTrue(done.isCalled());
		
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(done.getValue().getCursor())
			.setCount(10).build();
		controller = new ProtoRpcController();
		done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, done);
		assertTrue(done.isCalled());
		
		assertEquals(1, done.getValue().getResultCount());
		assertEquals("a", done.getValue().getResult(0).getKey().getPath().getElement(0).getName().toStringUtf8());
	}


	@Test
	public void testSinglePropertySort() throws ParseException, FileNotFoundException, IOException {
		// Tests that a sort query on a single property works.
		loadCorpus();
		
		RpcController controller = new ProtoRpcController();
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addOrder(DatastoreV3.Query.Order.newBuilder().setProperty(ByteString.copyFromUtf8("num")))
			.build();
		TestRpcCallback<DatastoreV3.QueryResult> done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, done);
		assertTrue(done.isCalled());
		
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(done.getValue().getCursor())
			.setCount(10).build();
		controller = new ProtoRpcController();
		done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, done);
		assertTrue(done.isCalled());
		
		assertEquals(4, done.getValue().getResultCount());
		assertEquals("b", done.getValue().getResult(0).getKey().getPath().getElement(0).getName().toStringUtf8());
		assertEquals("c", done.getValue().getResult(1).getKey().getPath().getElement(0).getName().toStringUtf8());
		assertEquals("a", done.getValue().getResult(2).getKey().getPath().getElement(0).getName().toStringUtf8());
		assertEquals("d", done.getValue().getResult(3).getKey().getPath().getElement(0).getName().toStringUtf8());
	}
	
	@Test
	public void testMergeJoinQuery() throws ParseException, FileNotFoundException, IOException {
		// Tests that a merge join query executes correctly
		loadCorpus();

		RpcController controller = new ProtoRpcController();
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.EQUAL.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("tags"))
					.setValue(Entity.PropertyValue.newBuilder().setStringValue(ByteString.copyFromUtf8("foo")))))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.EQUAL.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setValue(Entity.PropertyValue.newBuilder().setInt64Value(5))))
			.build();
		TestRpcCallback<DatastoreV3.QueryResult> done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, done);
		assertTrue(done.isCalled());
		
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(done.getValue().getCursor())
			.setCount(10).build();
		controller = new ProtoRpcController();
		done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, done);
		assertTrue(done.isCalled());
		
		assertEquals(1, done.getValue().getResultCount());
		assertEquals("a", done.getValue().getResult(0).getKey().getPath().getElement(0).getName().toStringUtf8());
	}
	
	@Test
	public void testEmptyResultSet() throws ParseException, FileNotFoundException, IOException {
		loadCorpus();

		RpcController controller = new ProtoRpcController();
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.EQUAL.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setValue(Entity.PropertyValue.newBuilder().setInt64Value(42))))
			.build();
		TestRpcCallback<DatastoreV3.QueryResult> done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, done);
		assertTrue(done.isCalled());
		assertTrue(done.getValue().getMoreResults());
		
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(done.getValue().getCursor())
			.setCount(10).build();
		controller = new ProtoRpcController();
		done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, done);
		assertTrue(done.isCalled());
		assertEquals(0, done.getValue().getResultCount());
		assertFalse(done.getValue().getMoreResults());
	}
	
	@Test
	public void testCount() throws ParseException, FileNotFoundException, IOException {
		loadCorpus();
		
		RpcController controller = new ProtoRpcController();
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addOrder(DatastoreV3.Query.Order.newBuilder().setProperty(ByteString.copyFromUtf8("num")))
			.build();
		TestRpcCallback<ApiBase.Integer64Proto> done = new TestRpcCallback<ApiBase.Integer64Proto>();
		service.count(controller, query, done);
		assertTrue(done.isCalled());
		assertEquals(4, done.getValue().getValue());
	}
	
	@Test
	public void testCompositeIndexGeneration() throws ParseException, FileNotFoundException, IOException, DatabaseException {
		RpcController controller = new ProtoRpcController();
		
		TestRpcCallback<ApiBase.Integer64Proto> done = new TestRpcCallback<ApiBase.Integer64Proto>();
		service.createIndex(controller, compositeIdx, done);
		
		loadCorpus();
		
		Object[] keyNames = new Object[] { "a", "b", "a", "b" };
		
		AppDatastore ds = this.service.datastore.getAppDatastore("testapp");
		SecondaryCursor cur = ds.indexes.get(compositeIdx.getDefinition()).openSecondaryCursor(null, null);
		List<String> resultNames = new ArrayList<String>();
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		while(cur.getNext(key, data, null) == OperationStatus.SUCCESS) {
			Entity.EntityProto entity = Entity.EntityProto.parseFrom(data.getData());
			resultNames.add(entity.getKey().getPath().getElement(0).getName().toStringUtf8());
		}

		assertArrayEquals(keyNames, resultNames.toArray());
		
		cur.close();
	}
	
	@Test
	public void testCompositeIndexQueries() throws ParseException, FileNotFoundException, IOException {
		RpcController controller = new ProtoRpcController();
		
		TestRpcCallback<ApiBase.Integer64Proto> done = new TestRpcCallback<ApiBase.Integer64Proto>();
		service.createIndex(controller, compositeIdx, done);
		
		loadCorpus();
		
		// Perform a basic composite index query
		DatastoreV3.Query query = DatastoreV3.Query.newBuilder()
			.setApp("testapp")
			.setKind(ByteString.copyFromUtf8("wtype"))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.EQUAL.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("tags"))
					.setValue(Entity.PropertyValue.newBuilder()
						.setStringValue(ByteString.copyFromUtf8("foo")))))
			.addFilter(DatastoreV3.Query.Filter.newBuilder()
				.setOp(DatastoreV3.Query.Filter.Operator.GREATER_THAN.getNumber())
				.addProperty(Entity.Property.newBuilder()
					.setName(ByteString.copyFromUtf8("num"))
					.setValue(Entity.PropertyValue.newBuilder()
						.setInt64Value(3))))
			.addOrder(DatastoreV3.Query.Order.newBuilder()
				.setProperty(ByteString.copyFromUtf8("num"))
				.setDirection(DatastoreV3.Query.Order.Direction.DESCENDING.getNumber()))
			.build();
		TestRpcCallback<DatastoreV3.QueryResult> query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, query_done);
		assertTrue(query_done.isCalled());
		assertTrue(service.cursors.containsKey(query_done.getValue().getCursor()));
		
		// Get the results
		controller = new ProtoRpcController();
		DatastoreV3.NextRequest next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(query_done.getValue().getCursor())
			.setCount(5).build();
		query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, query_done);
		assertTrue(query_done.isCalled());
		
		assertEquals(1, query_done.getValue().getResultCount());
		assertEquals("a", query_done.getValue().getResult(0).getKey().getPath().getElement(0).getName().toStringUtf8());
		
		// Perform a query with one filter and one order
		query = DatastoreV3.Query.newBuilder(query).clearFilter().addFilter(query.getFilter(0)).build();
		
		controller = new ProtoRpcController();
		query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.runQuery(controller, query, query_done);
		assertTrue(query_done.isCalled());
		
		// Get the results
		controller = new ProtoRpcController();
		next = DatastoreV3.NextRequest.newBuilder()
			.setCursor(query_done.getValue().getCursor())
			.setCount(5).build();
		query_done = new TestRpcCallback<DatastoreV3.QueryResult>();
		service.next(controller, next, query_done);
		assertTrue(query_done.isCalled());
		
		assertEquals(2, query_done.getValue().getResultCount());
	}
	
	@Test
	public void testCompositeIndexOrdering() {
		Indexing.CompositeIndexKey keyA = Indexing.CompositeIndexKey.newBuilder()
			.addValue(Entity.PropertyValue.newBuilder()
				.setStringValue(ByteString.copyFromUtf8("aaa")))
			.addValue(Entity.PropertyValue.newBuilder()
				.setInt64Value(2))
			.build();
		Indexing.CompositeIndexKey key3 = Indexing.CompositeIndexKey.newBuilder()
			.addValue(Entity.PropertyValue.newBuilder()
				.setStringValue(ByteString.copyFromUtf8("foo")))
			.addValue(Entity.PropertyValue.newBuilder()
				.setInt64Value(3))
			.build();
		Indexing.CompositeIndexKey key5 = Indexing.CompositeIndexKey.newBuilder()
			.addValue(Entity.PropertyValue.newBuilder()
				.setStringValue(ByteString.copyFromUtf8("foo")))
			.addValue(Entity.PropertyValue.newBuilder()
				.setInt64Value(5))
			.build();
		Indexing.CompositeIndexKey keyPrefix = Indexing.CompositeIndexKey.newBuilder()
			.addValue(Entity.PropertyValue.newBuilder()
				.setStringValue(ByteString.copyFromUtf8("foo")))
			.build();
		Indexing.CompositeIndexKey keyLast = Indexing.CompositeIndexKey.newBuilder()
			.addValue(Entity.PropertyValue.newBuilder()
				.setStringValue(ByteString.copyFromUtf8("foo")))
			.addValue(Entity.PropertyValue.getDefaultInstance())
			.build();
		Indexing.CompositeIndexKey keyZ = Indexing.CompositeIndexKey.newBuilder()
		.addValue(Entity.PropertyValue.newBuilder()
			.setStringValue(ByteString.copyFromUtf8("zzz")))
		.addValue(Entity.PropertyValue.newBuilder()
			.setInt64Value(10))
		.build();
		
		CompositeIndexKeyComparator comparator = new CompositeIndexKeyComparator(new int[] { 1, 1 }, false);
		assertTrue(comparator.compare(keyA, key3) < 0);
		assertTrue(comparator.compare(key3, key3) == 0);
		assertTrue(comparator.compare(key3, key5) < 0);
		assertTrue(comparator.compare(keyPrefix, key3) < 0);
		assertTrue(comparator.compare(key5, keyLast) < 0);
		assertTrue(comparator.compare(keyZ, keyLast) > 0);
		
		comparator = new CompositeIndexKeyComparator(new int[] { 1, -1 }, false);
		assertTrue(comparator.compare(keyA, key3) < 0);
		assertTrue(comparator.compare(key3, key3) == 0);
		assertTrue(comparator.compare(key3, key5) > 0);
		assertTrue(comparator.compare(keyPrefix, key3) > 0);
		assertTrue(comparator.compare(key5, keyLast) > 0);
		assertTrue(comparator.compare(keyZ, keyLast) > 0);
	}
}
