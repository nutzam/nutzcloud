package org.nutz.boot.starter.literpc.impl.endpoint.tio;

import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.HEADER_LENGHT;
import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.OP_PING;
import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.OP_RPC_REQ;
import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.OP_RPC_RESP;
import static org.nutz.boot.starter.literpc.impl.endpoint.tcp.LiteRpcTcpValues.VERSION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.nutz.boot.starter.literpc.LiteRpc;
import org.nutz.boot.starter.literpc.api.RpcResp;
import org.nutz.boot.starter.literpc.api.RpcSerializer;
import org.nutz.boot.starter.literpc.impl.RpcInvoker;
import org.nutz.ioc.impl.PropertiesProxy;
import org.nutz.ioc.loader.annotation.Inject;
import org.nutz.ioc.loader.annotation.IocBean;
import org.nutz.log.Log;
import org.nutz.log.Logs;
import org.tio.core.ChannelContext;
import org.tio.core.GroupContext;
import org.tio.core.Tio;
import org.tio.core.exception.AioDecodeException;
import org.tio.core.intf.Packet;
import org.tio.server.intf.ServerAioHandler;

@IocBean
public class TioRpcServerEndpoint implements ServerAioHandler {

    private static final Log log = Logs.get();

    @Inject
    protected PropertiesProxy conf;

    @Inject
    protected LiteRpc liteRpc;

    protected boolean debug;

    public void _init() {
        debug = conf.getBoolean("literpc.endpoint.tio.debug", false);
    }

    @Override
    public Packet decode(ByteBuffer buffer, int limit, int position, int readableLength, ChannelContext channelContext) throws AioDecodeException {
        if (readableLength < HEADER_LENGHT)
            return null;
        // 读取消息体的长度
        int bodyLength = buffer.getInt();

        // 数据不正确，则抛出AioDecodeException异常
        if (bodyLength < 2) {
            throw new AioDecodeException("bodyLength [" + bodyLength + "] is not right, remote:" + channelContext.getClientNode());
        }

        // 计算本次需要的数据长度
        int neededLength = HEADER_LENGHT + bodyLength;
        // 收到的数据是否足够组包
        int isDataEnough = readableLength - neededLength;
        // 不够消息体长度(剩下的buffe组不了消息体)
        if (isDataEnough < 0) {
            return null;
        } else // 组包成功
        {
            LiteRpcPacket rpcPacket = new LiteRpcPacket();
            // 版本信息占一个字节
            byte version = buffer.get();
            if (version != 1) {}
            // 操作类型占一个字节
            rpcPacket.opType = buffer.get();
            if (rpcPacket.opType == OP_PING) {
                // 心跳包没有剩余信息,清除多余的body,返回null
                if (bodyLength > 2)
                    buffer.clear();
                return null;
            }
            byte[] dst = new byte[bodyLength - 2];
            buffer.get(dst);
            // log.debug(Lang.fixedHexString(dst));
            rpcPacket.setBody(dst);
            return rpcPacket;
        }
    }

    @Override
    public ByteBuffer encode(Packet packet, GroupContext groupContext, ChannelContext channelContext) {
        LiteRpcPacket rpcPacket = (LiteRpcPacket) packet;
        // 理论上只会编码resp对象吧
        if (rpcPacket.opType == OP_RPC_RESP) {
            int bodySize = 2 + 16;
            if (rpcPacket.body != null) {
                bodySize += rpcPacket.body.length;
            }
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGHT + bodySize);
            buffer.order(groupContext.getByteOrder());
            buffer.putInt(bodySize);
            buffer.put(VERSION);
            buffer.put(OP_RPC_RESP);
            buffer.putLong(rpcPacket.uuidMost);
            buffer.putLong(rpcPacket.uuidLeast);
            if (rpcPacket.body != null)
                buffer.put(rpcPacket.body);
            return buffer;
        }
        return null;
    }

    @Override
    public void handler(Packet packet, ChannelContext channelContext) throws Exception {
        // 当前只有request啦
        LiteRpcPacket rpcPacket = (LiteRpcPacket) packet;
        if (rpcPacket.opType == OP_RPC_REQ) {
            // 读取类名
            DataInputStream ins = new DataInputStream(new ByteArrayInputStream(rpcPacket.body));
            rpcPacket.uuidMost = ins.readLong();
            rpcPacket.uuidLeast = ins.readLong();
            String klassName = ins.readUTF();
            String methodSign = ins.readUTF();
            String scName = ins.readUTF();
            RpcSerializer serializer = liteRpc.getSerializer(scName);
            RpcInvoker invoker = liteRpc.getInvoker(klassName, methodSign);
            Object[] args;
            try {
                args = (Object[]) serializer.read(ins);
            }
            catch (Throwable e) {
                sendRpcResp(channelContext, new RpcResp(e), serializer, rpcPacket);
                return;
            }
            RpcResp rpcResp = new RpcResp();
            try {
                rpcResp.returnValue = invoker.invoke(args);
            }
            catch (Throwable e) {
                rpcResp.err = e;
            }
            sendRpcResp(channelContext, rpcResp, serializer, rpcPacket);
        }
    }

    protected void sendRpcResp(ChannelContext channelContext, RpcResp resp, RpcSerializer serializer, LiteRpcPacket rpcPacket) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            if (resp.err != null) {
                out.write(2);
                serializer.write(resp.err, out);
            } else if (resp.returnValue == null) {
                out.write(0);
            } else {
                out.write(1);
                serializer.write(resp.returnValue, out);
            }
        }
        catch (Exception e) {
            if (debug && log.isDebugEnabled())
                log.debug(e.getMessage(), e);
            throw e;
        }
        rpcPacket.body = out.toByteArray();
        // log.debug(Lang.fixedHexString(rpcPacket.body));
        rpcPacket.opType = OP_RPC_RESP;
        Tio.send(channelContext, rpcPacket);
    }
}
