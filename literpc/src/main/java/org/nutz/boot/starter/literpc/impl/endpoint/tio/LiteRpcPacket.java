package org.nutz.boot.starter.literpc.impl.endpoint.tio;

import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.*;

import java.nio.ByteBuffer;

import org.tio.core.intf.Packet;

public class LiteRpcPacket extends Packet {

    private static final long serialVersionUID = 1L;

    protected byte[] body;

    protected int opType;

    protected long uuidMost, uuidLeast;

    public void setBody(byte[] body) {
        this.body = body;
    }

    public static byte[] PKG_PING;
    static {
        ByteBuffer buffer = ByteBuffer.allocate(6);
        buffer.putInt(2);
        buffer.put(VERSION);
        buffer.put(OP_PING);
        PKG_PING = buffer.array();
    }
}
