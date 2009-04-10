// Generated by the protocol buffer compiler.  DO NOT EDIT!

package net.notdot.protorpc;

public final class Rpc {
  private Rpc() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public static final class Request extends
      com.google.protobuf.GeneratedMessage {
    // Use Request.newBuilder() to construct.
    private Request() {}
    
    private static final Request defaultInstance = new Request();
    public static Request getDefaultInstance() {
      return defaultInstance;
    }
    
    public Request getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return net.notdot.protorpc.Rpc.internal_static_net_notdot_protorpc_Request_descriptor;
    }
    
    @Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return net.notdot.protorpc.Rpc.internal_static_net_notdot_protorpc_Request_fieldAccessorTable;
    }
    
    // required string method = 1;
    private boolean hasMethod;
    private java.lang.String method_ = "";
    public boolean hasMethod() { return hasMethod; }
    public java.lang.String getMethod() { return method_; }
    
    // required bytes body = 2;
    private boolean hasBody;
    private com.google.protobuf.ByteString body_ = com.google.protobuf.ByteString.EMPTY;
    public boolean hasBody() { return hasBody; }
    public com.google.protobuf.ByteString getBody() { return body_; }
    
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Request parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return new Builder(); }
    public Builder newBuilderForType() { return new Builder(); }
    public static Builder newBuilder(net.notdot.protorpc.Rpc.Request prototype) {
      return new Builder().mergeFrom(prototype);
    }
    
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> {
      // Construct using net.notdot.protorpc.Rpc.Request.newBuilder()
      private Builder() {}
      
      net.notdot.protorpc.Rpc.Request result = new net.notdot.protorpc.Rpc.Request();
      
      @Override
      protected net.notdot.protorpc.Rpc.Request internalGetResult() {
        return result;
      }
      
      @Override
      public Builder clear() {
        result = new net.notdot.protorpc.Rpc.Request();
        return this;
      }
      
      @Override
      public Builder clone() {
        return new Builder().mergeFrom(result);
      }
      
      @Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return net.notdot.protorpc.Rpc.Request.getDescriptor();
      }
      
      public net.notdot.protorpc.Rpc.Request getDefaultInstanceForType() {
        return net.notdot.protorpc.Rpc.Request.getDefaultInstance();
      }
      
      public net.notdot.protorpc.Rpc.Request build() {
        if (!isInitialized()) {
          throw new com.google.protobuf.UninitializedMessageException(
            result);
        }
        return buildPartial();
      }
      
      private net.notdot.protorpc.Rpc.Request buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        if (!isInitialized()) {
          throw new com.google.protobuf.UninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return buildPartial();
      }
      
      public net.notdot.protorpc.Rpc.Request buildPartial() {
        net.notdot.protorpc.Rpc.Request returnMe = result;
        result = null;
        return returnMe;
      }
      
      
      // required string method = 1;
      public boolean hasMethod() {
        return result.hasMethod();
      }
      public java.lang.String getMethod() {
        return result.getMethod();
      }
      public Builder setMethod(java.lang.String value) {
        result.hasMethod = true;
        result.method_ = value;
        return this;
      }
      public Builder clearMethod() {
        result.hasMethod = false;
        result.method_ = "";
        return this;
      }
      
      // required bytes body = 2;
      public boolean hasBody() {
        return result.hasBody();
      }
      public com.google.protobuf.ByteString getBody() {
        return result.getBody();
      }
      public Builder setBody(com.google.protobuf.ByteString value) {
        result.hasBody = true;
        result.body_ = value;
        return this;
      }
      public Builder clearBody() {
        result.hasBody = false;
        result.body_ = com.google.protobuf.ByteString.EMPTY;
        return this;
      }
    }
    
    static {
      net.notdot.protorpc.Rpc.getDescriptor();
    }
  }
  
  public static final class Response extends
      com.google.protobuf.GeneratedMessage {
    // Use Response.newBuilder() to construct.
    private Response() {}
    
    private static final Response defaultInstance = new Response();
    public static Response getDefaultInstance() {
      return defaultInstance;
    }
    
    public Response getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return net.notdot.protorpc.Rpc.internal_static_net_notdot_protorpc_Response_descriptor;
    }
    
    @Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return net.notdot.protorpc.Rpc.internal_static_net_notdot_protorpc_Response_fieldAccessorTable;
    }
    
    public static enum ResponseType {
      OK(0, 0),
      RPC_FAILED(1, 1),
      CALL_NOT_FOUND(2, 2),
      ARGUMENT_ERROR(3, 3),
      DEADLINE_EXCEEDED(4, 4),
      CANCELLED(5, 5),
      APPLICATION_ERROR(6, 6),
      OTHER_ERROR(7, 7),
      OVER_QUOTA(8, 8),
      REQUEST_TOO_LARGE(9, 9),
      CAPABILITY_DISABLED(10, 10),
      ;
      
      
      public final int getNumber() { return value; }
      
      public static ResponseType valueOf(int value) {
        switch (value) {
          case 0: return OK;
          case 1: return RPC_FAILED;
          case 2: return CALL_NOT_FOUND;
          case 3: return ARGUMENT_ERROR;
          case 4: return DEADLINE_EXCEEDED;
          case 5: return CANCELLED;
          case 6: return APPLICATION_ERROR;
          case 7: return OTHER_ERROR;
          case 8: return OVER_QUOTA;
          case 9: return REQUEST_TOO_LARGE;
          case 10: return CAPABILITY_DISABLED;
          default: return null;
        }
      }
      
      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(index);
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return net.notdot.protorpc.Rpc.Response.getDescriptor().getEnumTypes().get(0);
      }
      
      private static final ResponseType[] VALUES = {
        OK, RPC_FAILED, CALL_NOT_FOUND, ARGUMENT_ERROR, DEADLINE_EXCEEDED, CANCELLED, APPLICATION_ERROR, OTHER_ERROR, OVER_QUOTA, REQUEST_TOO_LARGE, CAPABILITY_DISABLED, 
      };
      public static ResponseType valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }
      private final int index;
      private final int value;
      private ResponseType(int index, int value) {
        this.index = index;
        this.value = value;
      }
      
      static {
        net.notdot.protorpc.Rpc.getDescriptor();
      }
    }
    
    // required .net.notdot.protorpc.Response.ResponseType status = 1 [default = OK];
    private boolean hasStatus;
    private net.notdot.protorpc.Rpc.Response.ResponseType status_ = net.notdot.protorpc.Rpc.Response.ResponseType.OK;
    public boolean hasStatus() { return hasStatus; }
    public net.notdot.protorpc.Rpc.Response.ResponseType getStatus() { return status_; }
    
    // optional bytes body = 2;
    private boolean hasBody;
    private com.google.protobuf.ByteString body_ = com.google.protobuf.ByteString.EMPTY;
    public boolean hasBody() { return hasBody; }
    public com.google.protobuf.ByteString getBody() { return body_; }
    
    // optional string error_detail = 3;
    private boolean hasErrorDetail;
    private java.lang.String errorDetail_ = "";
    public boolean hasErrorDetail() { return hasErrorDetail; }
    public java.lang.String getErrorDetail() { return errorDetail_; }
    
    // optional int32 application_error = 4;
    private boolean hasApplicationError;
    private int applicationError_ = 0;
    public boolean hasApplicationError() { return hasApplicationError; }
    public int getApplicationError() { return applicationError_; }
    
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static net.notdot.protorpc.Rpc.Response parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistry extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return new Builder(); }
    public Builder newBuilderForType() { return new Builder(); }
    public static Builder newBuilder(net.notdot.protorpc.Rpc.Response prototype) {
      return new Builder().mergeFrom(prototype);
    }
    
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> {
      // Construct using net.notdot.protorpc.Rpc.Response.newBuilder()
      private Builder() {}
      
      net.notdot.protorpc.Rpc.Response result = new net.notdot.protorpc.Rpc.Response();
      
      @Override
      protected net.notdot.protorpc.Rpc.Response internalGetResult() {
        return result;
      }
      
      @Override
      public Builder clear() {
        result = new net.notdot.protorpc.Rpc.Response();
        return this;
      }
      
      @Override
      public Builder clone() {
        return new Builder().mergeFrom(result);
      }
      
      @Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return net.notdot.protorpc.Rpc.Response.getDescriptor();
      }
      
      public net.notdot.protorpc.Rpc.Response getDefaultInstanceForType() {
        return net.notdot.protorpc.Rpc.Response.getDefaultInstance();
      }
      
      public net.notdot.protorpc.Rpc.Response build() {
        if (!isInitialized()) {
          throw new com.google.protobuf.UninitializedMessageException(
            result);
        }
        return buildPartial();
      }
      
      private net.notdot.protorpc.Rpc.Response buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        if (!isInitialized()) {
          throw new com.google.protobuf.UninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return buildPartial();
      }
      
      public net.notdot.protorpc.Rpc.Response buildPartial() {
        net.notdot.protorpc.Rpc.Response returnMe = result;
        result = null;
        return returnMe;
      }
      
      
      // required .net.notdot.protorpc.Response.ResponseType status = 1 [default = OK];
      public boolean hasStatus() {
        return result.hasStatus();
      }
      public net.notdot.protorpc.Rpc.Response.ResponseType getStatus() {
        return result.getStatus();
      }
      public Builder setStatus(net.notdot.protorpc.Rpc.Response.ResponseType value) {
        result.hasStatus = true;
        result.status_ = value;
        return this;
      }
      public Builder clearStatus() {
        result.hasStatus = false;
        result.status_ = net.notdot.protorpc.Rpc.Response.ResponseType.OK;
        return this;
      }
      
      // optional bytes body = 2;
      public boolean hasBody() {
        return result.hasBody();
      }
      public com.google.protobuf.ByteString getBody() {
        return result.getBody();
      }
      public Builder setBody(com.google.protobuf.ByteString value) {
        result.hasBody = true;
        result.body_ = value;
        return this;
      }
      public Builder clearBody() {
        result.hasBody = false;
        result.body_ = com.google.protobuf.ByteString.EMPTY;
        return this;
      }
      
      // optional string error_detail = 3;
      public boolean hasErrorDetail() {
        return result.hasErrorDetail();
      }
      public java.lang.String getErrorDetail() {
        return result.getErrorDetail();
      }
      public Builder setErrorDetail(java.lang.String value) {
        result.hasErrorDetail = true;
        result.errorDetail_ = value;
        return this;
      }
      public Builder clearErrorDetail() {
        result.hasErrorDetail = false;
        result.errorDetail_ = "";
        return this;
      }
      
      // optional int32 application_error = 4;
      public boolean hasApplicationError() {
        return result.hasApplicationError();
      }
      public int getApplicationError() {
        return result.getApplicationError();
      }
      public Builder setApplicationError(int value) {
        result.hasApplicationError = true;
        result.applicationError_ = value;
        return this;
      }
      public Builder clearApplicationError() {
        result.hasApplicationError = false;
        result.applicationError_ = 0;
        return this;
      }
    }
    
    static {
      net.notdot.protorpc.Rpc.getDescriptor();
    }
  }
  
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_net_notdot_protorpc_Request_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_net_notdot_protorpc_Request_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_net_notdot_protorpc_Response_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_net_notdot_protorpc_Response_fieldAccessorTable;
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String descriptorData =
      "\n\trpc.proto\022\023net.notdot.protorpc\"\'\n\007Requ" +
      "est\022\016\n\006method\030\001 \002(\t\022\014\n\004body\030\002 \002(\014\"\350\002\n\010Re" +
      "sponse\022>\n\006status\030\001 \002(\0162*.net.notdot.prot" +
      "orpc.Response.ResponseType:\002OK\022\014\n\004body\030\002" +
      " \001(\014\022\024\n\014error_detail\030\003 \001(\t\022\031\n\021applicatio" +
      "n_error\030\004 \001(\005\"\334\001\n\014ResponseType\022\006\n\002OK\020\000\022\016" +
      "\n\nRPC_FAILED\020\001\022\022\n\016CALL_NOT_FOUND\020\002\022\022\n\016AR" +
      "GUMENT_ERROR\020\003\022\025\n\021DEADLINE_EXCEEDED\020\004\022\r\n" +
      "\tCANCELLED\020\005\022\025\n\021APPLICATION_ERROR\020\006\022\017\n\013O" +
      "THER_ERROR\020\007\022\016\n\nOVER_QUOTA\020\010\022\025\n\021REQUEST_" +
      "TOO_LARGE\020\t\022\027\n\023CAPABILITY_DISABLED\020\n";
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_net_notdot_protorpc_Request_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_net_notdot_protorpc_Request_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_net_notdot_protorpc_Request_descriptor,
              new java.lang.String[] { "Method", "Body", },
              net.notdot.protorpc.Rpc.Request.class,
              net.notdot.protorpc.Rpc.Request.Builder.class);
          internal_static_net_notdot_protorpc_Response_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_net_notdot_protorpc_Response_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_net_notdot_protorpc_Response_descriptor,
              new java.lang.String[] { "Status", "Body", "ErrorDetail", "ApplicationError", },
              net.notdot.protorpc.Rpc.Response.class,
              net.notdot.protorpc.Rpc.Response.Builder.class);
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }
}
