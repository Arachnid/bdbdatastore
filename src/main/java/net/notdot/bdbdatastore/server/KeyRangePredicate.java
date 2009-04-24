package net.notdot.bdbdatastore.server;

import net.notdot.bdbdatastore.Indexing;

import com.google.protobuf.Message;

public class KeyRangePredicate implements MessagePredicate {
	protected Indexing.EntityKey endKey;
	protected int maxVal;

	public KeyRangePredicate(Indexing.EntityKey endKey, boolean upperExclusive) {
		this.endKey = endKey;
		this.maxVal = upperExclusive?-1:0;
	}
	
	public boolean evaluate(Message msg) {
		Indexing.EntityKey testkey = (Indexing.EntityKey)msg;
		return EntityKeyComparator.instance.compare(testkey, this.endKey) <= this.maxVal;
	}

}
