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
		if(o1.getValueCount() != this.directions.length) {
			logger.error("Attempting to compare key of unexpected length (expected {}): {}",
					this.directions.length, o1);
			return 0;
		} else if(o2.getValueCount() != this.directions.length) {
			logger.error("Attempting to compare key of unexpected length (expected {}): {}",
					this.directions.length, o2);
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
		for(int i = 0; i < this.directions.length; i++) {
			ret = PropertyValueComparator.instance.compare(o1.getValue(i), o2.getValue(i));
			if(ret != 0)
				return ret * this.directions[i];
		}
		return 0;
	}
}
