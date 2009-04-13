package net.notdot.bdbdatastore.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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
		TextFormat.merge(new FileReader("test/dataset.txt"), request);
		dataset_put = request.build();
	}
	
	@After
	public void cleanUp() {
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
		Entity.Reference startKey = Entity.Reference.newBuilder()
		.setApp("testapp")
		.setPath(Entity.Path.newBuilder()
				.addElement(Entity.Path.Element.newBuilder()
						.setType(ByteString.copyFromUtf8("testtype")))).build();
		assertTrue(ReferenceComparator.instance.compare(startKey, dataset_put.getEntity(0).getKey()) > 0);
		assertTrue(ReferenceComparator.instance.compare(startKey, dataset_put.getEntity(1).getKey()) < 0);
		assertTrue(ReferenceComparator.instance.compare(startKey, dataset_put.getEntity(2).getKey()) < 0);
		assertTrue(ReferenceComparator.instance.compare(startKey, dataset_put.getEntity(3).getKey()) < 0);
	}
	
	@Test
	public void testEntityOrdering() throws ParseException, FileNotFoundException, IOException, DatabaseException {
		loadCorpus();
		AppDatastore ds = this.service.datastore.getAppDatastore("testapp");
		Cursor cur = ds.entities.openCursor(null, null);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		cur.getFirst(key, data, null);
		assertEquals(dataset_put.getEntity(0).getKey(), Entity.Reference.parseFrom(key.getData()));
		assertEquals(dataset_put.getEntity(0), Entity.EntityProto.parseFrom(data.getData()));
		
		for(int i = 1; i < 4; i++) {
			cur.getNext(key, data, null);
			assertEquals(dataset_put.getEntity(i).getKey(), Entity.Reference.parseFrom(key.getData()));
			assertEquals(dataset_put.getEntity(i), Entity.EntityProto.parseFrom(data.getData()));
		}
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
}
