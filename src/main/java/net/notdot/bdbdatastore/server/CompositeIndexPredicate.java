package net.notdot.bdbdatastore.server;

import net.notdot.bdbdatastore.Indexing;
import net.notdot.bdbdatastore.Indexing.CompositeIndexKey;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.Index.Property.Direction;
import com.google.protobuf.Message;

public class CompositeIndexPredicate implements MessagePredicate {
	private CompositeIndexKeyComparator comparator;
	private CompositeIndexKey upperBound;
	int maxval;
	
	public CompositeIndexPredicate(Entity.Index idx, CompositeIndexKey upperBound, boolean exclusiveMax) {
		int[] directions = new int[idx.getPropertyCount()];
		for(int i = 0; i < directions.length; i++)
			directions[i] = (idx.getProperty(i).getDirection()==Direction.ASCENDING)?1:-1;
		this.comparator = new CompositeIndexKeyComparator(directions, idx.getAncestor());
		this.upperBound = upperBound;
		maxval = exclusiveMax?-1:0;
	}

	public boolean evaluate(Message msg) {
		Indexing.CompositeIndexKey key = (Indexing.CompositeIndexKey)msg;

		int cmp = this.comparator.compare(key, this.upperBound);
		return cmp <= this.maxval;
	}

}
