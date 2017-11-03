/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation; either version 3.0 of the
 * License, or (at your option) any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.waarp.gateway.kernel.exec;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import org.waarp.commandexec.client.LocalExecClientHandler;
import org.waarp.commandexec.client.LocalExecClientInitializer;
import org.waarp.commandexec.utils.LocalExecResult;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.future.WaarpFuture;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.utility.WaarpNettyUtil;
import org.waarp.common.utility.WaarpThreadFactory;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Client to execute external command through Waarp Local Exec
 * 
 * @author Frederic Bregier
 * 
 */
public class LocalExecClient {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(LocalExecClient.class);

    private static InetSocketAddress address;
    // Configure the client.
    static private Bootstrap bootstrapLocalExec;
    // Configure the pipeline factory.
    static private LocalExecClientInitializer localExecClientInitializer;
    static private EventLoopGroup localPipelineExecutor;

    /**
     * Initialize the LocalExec Client context
     */
    public static void initialize(int CLIENT_THREAD, long maxGlobalMemory) {
        localPipelineExecutor = new NioEventLoopGroup(CLIENT_THREAD * 100,
                new WaarpThreadFactory("LocalExecutor"));
        // Configure the client.
        bootstrapLocalExec = new Bootstrap();
        WaarpNettyUtil.setBootstrap(bootstrapLocalExec, localPipelineExecutor,
                (int) Configuration.configuration.getTIMEOUTCON());
        // Configure the pipeline factory.
        localExecClientInitializer = new LocalExecClientInitializer();
        bootstrapLocalExec.handler(localExecClientInitializer);
    }

    /**
     * To be called when the server is shutting down to release the resources
     */
    public static void releaseResources() {
        if (bootstrapLocalExec == null) {
            return;
        }
        // Shut down all thread pools to exit.
        bootstrapLocalExec.group().shutdownGracefully();
        localExecClientInitializer.releaseResources();
    }

    private Channel channel;
    private LocalExecResult result;

    public LocalExecClient() {

    }

    public LocalExecResult getLocalExecResult() {
        return result;
    }

    /**
     * Run one command with a specific allowed delay for execution. The connection must be ready
     * (done with connect()).
     * 
     * @param command
     * @param delay
     * @param futureCompletion
     */
    public void runOneCommand(String command, long delay, WaarpFuture futureCompletion) {
        // Initialize the command context
        LocalExecClientHandler clientHandler =
                (LocalExecClientHandler) channel.pipeline().last();
        // Command to execute
        clientHandler.initExecClient(delay, command);
        // Wait for the end of the exec command
        LocalExecResult localExecResult = clientHandler.waitFor(delay * 2);
        result = localExecResult;
        if (futureCompletion == null) {
            return;
        }
        if (result.getStatus() == 0) {
            futureCompletion.setSuccess();
            logger.info("Exec OK with {}", command);
        } else if (result.getStatus() == 1) {
            logger.warn("Exec in warning with {}", command);
            futureCompletion.setSuccess();
        } else {
            logger.error("Status: " + result.getStatus() + " Exec in error with " +
                    command + "\n" + result.getResult());
            futureCompletion.cancel();
        }
    }

    /**
     * Connect to the Server
     */
    public boolean connect() {
        // Start the connection attempt.
        ChannelFuture future = bootstrapLocalExec.connect(getAddress());

        // Wait until the connection attempt succeeds or fails.
        try {
            channel = future.await().sync().channel();
        } catch (InterruptedException e) {
        }
        if (!future.isSuccess()) {
            logger.error("Client Not Connected", future.cause());
            return false;
        }
        return true;
    }

    /**
     * Disconnect from the server
     */
    public void disconnect() {
        // Close the connection. Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        try {
            WaarpSslUtility.closingSslChannel(channel).await(Configuration.configuration.getTIMEOUTCON());
        } catch (InterruptedException e) {
        }
    }

    /**
     * @return the address
     */
    public static InetSocketAddress getAddress() {
        return address;
    }

    /**
     * @param address the address to set
     */
    public static void setAddress(InetSocketAddress address) {
        LocalExecClient.address = address;
    }
}
