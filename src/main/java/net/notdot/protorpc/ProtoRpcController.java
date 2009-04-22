package net.notdot.protorpc;

import net.notdot.bdbdatastore.server.DatastoreServer;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;

public class ProtoRpcController implements com.google.protobuf.RpcController {
	private static final Logger logger = LoggerFactory.getLogger(DatastoreServer.class);

	private Channel channel;
	private String service;
	private String method;
	private String client;
	private long rpcId;
	private long startTime;
	private String error = "";
	private int applicationError = 0;
	private boolean responseSent = false;
	
	public ProtoRpcController(Channel ch, String service, String method, long rpcId) {
		this.channel = ch;
		this.service = service;
		this.method = method;
		this.client = ch.getRemoteAddress().toString();
		this.rpcId = rpcId;
		this.startTime = System.currentTimeMillis();
	}
	
	public long getRpcId() {
		return this.rpcId;
	}
	
	public int getApplicationError() {
		return applicationError;
	}

	public String errorText() {
		return error;
	}

	public boolean failed() {
		return error != null;
	}

	public boolean isCanceled() {
		return false;
	}

	public void notifyOnCancel(RpcCallback<Object> arg0) {
	}

	public void reset() {
		
	}

	public void setFailed(String reason) {
		setFailed(reason, -1);
	}
	
	public void sendResponse(Rpc.Response response) {
		if(!this.responseSent) {
			this.channel.write(response);
			logger.info("{} {} {}.{} {}ms {} ({}) {}", new Object[] {
					this.client, this.rpcId, this.service, this.method,
					System.currentTimeMillis() - this.startTime,
					response.getStatus(), response.getApplicationError(),
					response.getErrorDetail() });
			logger.debug("Sent response to RPC {} for client {}: {}",
					new Object[] { this.rpcId, this.client, response });
			this.responseSent = true;
		} else {
			logger.error("Attempted to send multiple responses for RPC {}.{}", this.service, this.method);
		}
	}
	
	public boolean isResponseSent() {
		return responseSent;
	}

	public void setFailed(String reason, int applicationError) {
		Rpc.Response.Builder response = Rpc.Response.newBuilder();
		response.setRpcId(this.rpcId);
		if(applicationError >= 0) {
			response.setStatus(Rpc.Response.ResponseType.APPLICATION_ERROR);
			response.setApplicationError(applicationError);
		} else {
			response.setStatus(Rpc.Response.ResponseType.RPC_FAILED);
		}
		response.setErrorDetail(reason);
		this.sendResponse(response.build());
	}

	public void startCancel() {
	}
}
