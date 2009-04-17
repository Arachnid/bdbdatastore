package net.notdot.bdbdatastore.server;

import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.protobuf.Message;

public class KeyPredicate implements MessagePredicate {
	protected Indexing.EntityKey key;

	public KeyPredicate(Indexing.EntityKey key) {
		this.key = key;
	}
	public boolean evaluate(Message msg) {
		Indexing.EntityKey testkey = (Indexing.EntityKey)msg;
		Entity.Path path = testkey.getPath();

		if(this.key.hasPath()) {
			if(this.key.getPath().getElementCount() > testkey.getPath().getElementCount())
				return false;
			for(int i = 0; i < this.key.getPath().getElementCount(); i++) {
				if(!this.key.getPath().getElement(i).equals(path.getElement(i)))
					return false;
			}
		}

		Entity.Path.Element lastel = path.getElement(path.getElementCount() - 1);
		if(!this.key.getKind().equals(lastel.getType()))
			return false;
		
		return true;
	}

}
