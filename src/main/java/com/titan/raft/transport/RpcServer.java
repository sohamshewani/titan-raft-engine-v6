package com.titan.raft.transport;

import com.titan.raft.protocol.RaftMessages;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class RpcServer {

    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private final int port;
    private final Function<ByteBuffer, ByteBuffer> rpcDispatcher;
    private final AtomicBoolean running;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RpcServer(int port, Function<ByteBuffer, ByteBuffer> rpcDispatcher) {
        this.port = port;
        this.rpcDispatcher = rpcDispatcher;
        this.running = new AtomicBoolean(false);
    }

    public synchronized void start() throws InterruptedException {
        if (!running.compareAndSet(false, true)) {
            logger.warn("RpcServer is already running.");
            return;
        }

        boolean useEpoll = Epoll.isAvailable();
        if (useEpoll) {
            logger.info("Initializing RpcServer with Netty Epoll transport.");
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } else {
            logger.info("Initializing RpcServer with Netty NIO transport.");
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
        }

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 1024)
             .option(ChannelOption.SO_REUSEADDR, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline pipeline = ch.pipeline();
                     // Frame decoding/encoding: 4-byte length header prefix
                     pipeline.addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                     pipeline.addLast(new LengthFieldPrepender(4));
                     pipeline.addLast(new RpcServerHandler(rpcDispatcher));
                 }
             });

            ChannelFuture f = b.bind(new InetSocketAddress(port)).sync();
            serverChannel = f.channel();
            logger.info("RpcServer successfully bound and listening on port {}", port);
        } catch (InterruptedException e) {
            running.set(false);
            stopInternal();
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            running.set(false);
            stopInternal();
            throw new RuntimeException("Failed to start RpcServer on port " + port, e);
        }
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        logger.info("Stopping RpcServer on port {}...", port);
        stopInternal();
        logger.info("RpcServer stopped.");
    }

    private void stopInternal() {
        if (serverChannel != null) {
            try {
                serverChannel.close().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while closing server channel", e);
            }
            serverChannel = null;
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    @ChannelHandler.Sharable
    private static final class RpcServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final Function<ByteBuffer, ByteBuffer> dispatcher;

        private RpcServerHandler(Function<ByteBuffer, ByteBuffer> dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            try {
                int readableBytes = msg.readableBytes();
                if (readableBytes <= 0) {
                    logger.warn("Received empty RPC frame from {}", ctx.channel().remoteAddress());
                    return;
                }

                // Zero-copy or direct buffer extraction to preserve DirectByteBuffer pipeline
                ByteBuffer requestBuffer;
                if (msg.hasDirectBuffer()) {
                    requestBuffer = msg.internalNioBuffer(msg.readerIndex(), readableBytes);
                } else {
                    requestBuffer = ByteBuffer.allocateDirect(readableBytes);
                    msg.getBytes(msg.readerIndex(), requestBuffer);
                    requestBuffer.flip();
                }

                ByteBuffer responseBuffer = dispatcher.apply(requestBuffer);
                if (responseBuffer != null) {
                    ByteBuf responseByteBuf;
                    if (responseBuffer.isDirect()) {
                        responseByteBuf = Unpooled.wrappedBuffer(responseBuffer);
                    } else {
                        responseByteBuf = Unpooled.copiedBuffer(responseBuffer);
                    }
                    ctx.writeAndFlush(responseByteBuf);
                } else {
                    // Send empty acknowledgement if dispatcher yields null
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
                }
            } catch (Exception e) {
                logger.error("Exception handling inbound RPC from {}", ctx.channel().remoteAddress(), e);
                // Construct error response payload according to RaftMessages error envelope
                try {
                    ByteBuffer errResponse = RaftMessages.serializeError(e.getMessage() != null ? e.getMessage() : "Unknown Server Error");
                    ByteBuf errByteBuf = errResponse.isDirect() ? Unpooled.wrappedBuffer(errResponse) : Unpooled.copiedBuffer(errResponse);
                    ctx.writeAndFlush(errByteBuf);
                } catch (Exception innerEx) {
                    logger.error("Failed to write error response envelope", innerEx);
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Network transport exception on channel {}: {}", ctx.channel().remoteAddress(), cause.getMessage(), cause);
            ctx.close();
        }
    }
}