package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import com.google.protobuf.ByteString;

public class ByteStringComparator implements Comparator<ByteString> {
	public final static ByteStringComparator instance = new ByteStringComparator();

	public int compare(ByteString o1, ByteString o2) {
		return o1.asReadOnlyByteBuffer().compareTo(o2.asReadOnlyByteBuffer());
	}

}
