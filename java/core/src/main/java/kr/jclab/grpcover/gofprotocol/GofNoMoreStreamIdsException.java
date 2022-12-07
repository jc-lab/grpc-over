package kr.jclab.grpcover.gofprotocol;

import io.grpc.internal.GrpcUtil;

public class GofNoMoreStreamIdsException extends GofException {
    public GofNoMoreStreamIdsException() {
        super(GrpcUtil.Http2Error.NO_ERROR);
    }
}
