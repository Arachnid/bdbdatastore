package net.notdot.bdbdatastore.server;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.google.appengine.entity.Entity;
import com.google.protobuf.InvalidProtocolBufferException;

import net.notdot.bdbdatastore.server.ReferenceComparator;

public class SerializedReferenceComparator implements Comparator<byte[]> {
	public int compare(byte[] o1, byte[] o2) {
		try {
			return ReferenceComparator.instance.compare(Entity.Reference.parseFrom(o1), Entity.Reference.parseFrom(o2));
		} catch (InvalidProtocolBufferException e) {
			return ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
		}
	}
}
