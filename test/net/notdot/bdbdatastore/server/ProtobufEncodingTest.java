package net.notdot.bdbdatastore.server;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;


public class ProtobufEncodingTest {
	@Test
	public void testPathPrefix() {
		// Demonstrates that a path that is a prefix of another is
		// also a prefix in its byte representation.
		Entity.Path parent = Entity.Path.newBuilder()
				.addElement(Entity.Path.Element.newBuilder()
							.setType(ByteString.copyFromUtf8("testtype"))
							.setId(1)).build();
		Entity.Path child = Entity.Path.newBuilder(parent)
				.addElement(Entity.Path.Element.newBuilder()
							.setType(ByteString.copyFromUtf8("anothertype"))
							.setId(1)).build();

		assertEquals(parent.getElement(0), child.getElement(0));
		
		byte[] parentBytes = parent.toByteArray();
		byte[] childBytes = new byte[parentBytes.length];
		System.arraycopy(child.toByteArray(), 0, childBytes, 0, parentBytes.length);
		assertArrayEquals(parentBytes, childBytes);
	}
}
