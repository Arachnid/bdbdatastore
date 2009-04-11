package net.notdot.bdbdatastore.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import net.notdot.protorpc.ProtoRpcController;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.base.ApiBase.VoidProto;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

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
				.setName(ByteString.copyFromUtf8("foo"))
				.setValue(Entity.PropertyValue.newBuilder().setInt64Value(1234)))
		.addProperty(Entity.Property.newBuilder()
				.setName(ByteString.copyFromUtf8("bar"))
				.setValue(Entity.PropertyValue.newBuilder().setStringValue(ByteString.copyFromUtf8("Hello, world!")))).build();
	
	// Sample entity with no ID
	protected static Entity.EntityProto testnewent = Entity.EntityProto.newBuilder(testent)
		.setKey(Entity.Reference.newBuilder().setApp("testapp").setPath(
				Entity.Path.newBuilder().addElement(
						Entity.Path.Element.newBuilder().setType(ByteString.copyFromUtf8("testtype"))))).build();
	
	protected File basedir;
	protected Datastore datastore;
	protected DatastoreService service;
	protected Entity.Reference sample_key = null;
	
	@Before
	public void setUp() throws IOException {
		basedir = File.createTempFile("bdbdatastore", "tmp", new File(System.getProperty("java.io.tmpdir")));
		basedir.delete();
		basedir.mkdir();
		datastore = new Datastore(basedir.getAbsolutePath());
		service = new DatastoreService(datastore);
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
		fail("Not yet implemented");
	}

	@Test
	public void testDelete() {
		fail("Not yet implemented");
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
		assertEquals(sample_key.getPath().getElementCount(), 1);
		assertEquals(sample_key.getPath().getElement(0).getType(), testkey.getPath().getElement(0).getType());
		assertTrue(sample_key.getPath().getElement(0).hasId());
		assertTrue(sample_key.getPath().getElement(0).getId() > 0);
	}

	@Test
	public void testRollback() {
		fail("Not yet implemented");
	}
}
