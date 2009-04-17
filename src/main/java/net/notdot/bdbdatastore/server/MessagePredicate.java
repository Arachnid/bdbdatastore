package net.notdot.bdbdatastore.server;

import com.google.protobuf.Message;

public interface MessagePredicate {
	public boolean evaluate(Message msg);
}
