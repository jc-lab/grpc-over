package kr.jclab.grpcover.gofprotocol;

public interface FrameSizePolicy {
    void maxFrameSize(int max) throws GofException;
    int maxFrameSize();
}
