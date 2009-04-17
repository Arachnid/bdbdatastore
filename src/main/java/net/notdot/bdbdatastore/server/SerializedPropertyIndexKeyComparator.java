package net.notdot.bdbdatastore.server;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.google.protobuf.InvalidProtocolBufferException;

import net.notdot.bdbdatastore.Indexing;

public class SerializedPropertyIndexKeyComparator implements Comparator<byte[]> {
	public static final SerializedPropertyIndexKeyComparator instance = new SerializedPropertyIndexKeyComparator();
	public int compare(byte[] o1, byte[] o2) {
		try {
			return PropertyIndexKeyComparator.instance.compare(
					Indexing.PropertyIndexKey.parseFrom(o1),
					Indexing.PropertyIndexKey.parseFrom(o2));
		} catch (InvalidProtocolBufferException e) {
			return ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
		}
	}
}
