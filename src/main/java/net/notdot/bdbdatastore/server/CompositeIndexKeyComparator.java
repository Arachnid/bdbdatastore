package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.bdbdatastore.Indexing;

public class CompositeIndexKeyComparator implements
		Comparator<Indexing.CompositeIndexKey> {
	private static final Logger logger = LoggerFactory.getLogger(AppDatastore.class);

	private int[] directions;
	private boolean hasAncestor;
	
	public CompositeIndexKeyComparator(int[] directions, boolean hasAncestor) {
		this.directions = directions;
		this.hasAncestor = hasAncestor;
	}
	
	public int compare(Indexing.CompositeIndexKey o1, Indexing.CompositeIndexKey o2) {
		if(o1.getValueCount() > this.directions.length) {
			logger.error("Key {} is longer than expected length ({})",
					o1, this.directions.length);
			return 0;
		} else if(o2.getValueCount() > this.directions.length) {
			logger.error("Key {} is longer than expected length ({})",
					o2, this.directions.length);
			return 0;
		}
		if(this.hasAncestor != o1.hasAncestor() || this.hasAncestor != o2.hasAncestor()) {
			logger.error("Index has ancestor ({}) and entries have ancestor ({}, {})",
					new Object[] {this.hasAncestor, o1.hasAncestor(), o2.hasAncestor()});
			return 0;
		}
		
		int ret = EntityKeyComparator.comparePaths(o1.getAncestor(), o2.getAncestor());
		if(ret != 0)
			return ret;
		int minLength = Math.min(o1.getValueCount(), o2.getValueCount());
		for(int i = 0; i < minLength; i++) {
			ret = PropertyValueComparator.instance.compare(o1.getValue(i), o2.getValue(i));
			if(ret != 0)
				return ret * this.directions[i];
		}
		if(minLength < this.directions.length) {
			return this.directions[minLength] * (o1.getValueCount() - o2.getValueCount());
		} else {
			return o1.getValueCount() - o2.getValueCount();
		}
	}
}
