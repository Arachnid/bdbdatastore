package net.notdot.bdbdatastore.server;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.Index.Property.Direction;
import com.google.protobuf.InvalidProtocolBufferException;

import net.notdot.bdbdatastore.Indexing;

public class SerializedCompositeIndexKeyComparator implements Comparator<byte[]>, Serializable {
	private static final long serialVersionUID = -7219868790667839187L;
	
	private int[] directions;
	private boolean hasAncestor;
	private transient CompositeIndexKeyComparator comparator = null;
	
	public SerializedCompositeIndexKeyComparator(Entity.Index idx) {
		directions = new int[idx.getPropertyCount()];
		for(int i = 0; i < directions.length; i++)
			directions[i] = (idx.getProperty(i).getDirection()==Direction.ASCENDING)?1:-1;
		this.hasAncestor = idx.getAncestor();
	}
	
	public int compare(byte[] o1, byte[] o2) {
		if(this.comparator == null)
			this.comparator = new CompositeIndexKeyComparator(this.directions, this.hasAncestor);
		try {
			return this.comparator.compare(
					Indexing.CompositeIndexKey.parseFrom(o1),
					Indexing.CompositeIndexKey.parseFrom(o2));
		} catch (InvalidProtocolBufferException e) {
			return ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
		}
	}
}
