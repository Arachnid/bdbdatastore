package net.notdot.bdbdatastore.server;

import net.notdot.bdbdatastore.Indexing;

import com.google.protobuf.Message;

public class PropertyIndexPredicate implements MessagePredicate {
	Indexing.PropertyIndexKey upperBound;
	int maxval;
	
	public PropertyIndexPredicate(Indexing.PropertyIndexKey upperBound, boolean exclusiveMax) {
		this.upperBound = upperBound;
		maxval = exclusiveMax?-1:0;
	}

	public boolean evaluate(Message msg) {
		Indexing.PropertyIndexKey key = (Indexing.PropertyIndexKey)msg;
		
		// Kind must be equal
		if(!key.getKind().equals(this.upperBound.getKind()))
			return false;
		
		// Name must be equal
		if(!key.getName().equals(this.upperBound.getName()))
			return false;
		
		// If the upper bound has no value, there is no upper bound for this kind/name.
		if(!this.upperBound.hasValue())
			return true;
		
		// Otherwise, check if the upper bound is met.
		int cmp = PropertyValueComparator.instance.compare(key.getValue(), this.upperBound.getValue());
		return cmp <= this.maxval;
	}
}
