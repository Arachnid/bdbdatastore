package net.notdot.bdbdatastore.server;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.google.protobuf.InvalidProtocolBufferException;

import net.notdot.bdbdatastore.Indexing;
import net.notdot.bdbdatastore.server.EntityKeyComparator;

public class SerializedEntityKeyComparator implements Comparator<byte[]> {
	public int compare(byte[] o1, byte[] o2) {
		try {
			return EntityKeyComparator.instance.compare(Indexing.EntityKey.parseFrom(o1), Indexing.EntityKey.parseFrom(o2));
		} catch (InvalidProtocolBufferException e) {
			return ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
		}
	}
}
