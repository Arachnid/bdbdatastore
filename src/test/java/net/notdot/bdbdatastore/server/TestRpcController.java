package net.notdot.bdbdatastore.server;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class TestRpcController implements RpcController {

	public String errorText() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean failed() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isCanceled() {
		// TODO Auto-generated method stub
		return false;
	}

	public void notifyOnCancel(RpcCallback<Object> arg0) {
		// TODO Auto-generated method stub

	}

	public void reset() {
		// TODO Auto-generated method stub

	}

	public void setFailed(String arg0) {
		// TODO Auto-generated method stub

	}

	public void startCancel() {
		// TODO Auto-generated method stub

	}

}
