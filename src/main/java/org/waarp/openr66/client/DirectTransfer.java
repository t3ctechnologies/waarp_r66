/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.client;

import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.client.utils.OutputFormat;
import org.waarp.openr66.client.utils.OutputFormat.FIELDS;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotYetConnectionException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Direct Transfer from a client with or without database connection
 * 
 * @author Frederic Bregier
 * 
 */
public class DirectTransfer extends AbstractTransfer {
    protected final NetworkTransaction networkTransaction;

    public DirectTransfer(R66Future future, String remoteHost,
            String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
            long id,
            NetworkTransaction networkTransaction) {
        // no starttime since it is direct (blocking request, no delay)
        super(DirectTransfer.class,
                future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize, id, null);
        this.networkTransaction = networkTransaction;
    }

    /**
     * Prior to call this method, the pipeline and NetworkTransaction must have been initialized. It
     * is the responsibility of the caller to finish all network resources.
     */
    public void run() {
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(DirectTransfer.class);
        }
        DbTaskRunner taskRunner = this.initRequest();
        if (taskRunner == null) {
            // already an error from there
            return;
        }
        ClientRunner runner = new ClientRunner(networkTransaction, taskRunner, future);
        OpenR66ProtocolNotYetConnectionException exc = null;
        for (int i = 0; i < Configuration.RETRYNB; i++) {
            try {
                runner.runTransfer();
                exc = null;
                break;
            } catch (OpenR66RunnerErrorException e) {
                logger.debug("Cannot Transfer", e);
                future.setResult(new R66Result(e, null, true,
                        ErrorCode.Internal, taskRunner));
                future.setFailure(e);
                return;
            } catch (OpenR66ProtocolNoConnectionException e) {
                logger.debug("Cannot Connect", e);
                future.setResult(new R66Result(e, null, true,
                        ErrorCode.ConnectionImpossible, taskRunner));
                finalizeInErrorTransferRequest(runner, taskRunner, ErrorCode.ConnectionImpossible);
                // since no connection : just forget it
                if (nolog || taskRunner.shallIgnoreSave()) {
                    try {
                        taskRunner.delete();
                    } catch (WaarpDatabaseException e1) {
                    }
                }
                future.setFailure(e);
                return;
            } catch (OpenR66ProtocolPacketException e) {
                logger.debug("Bad Protocol", e);
                future.setResult(new R66Result(e, null, true,
                        ErrorCode.TransferError, taskRunner));
                future.setFailure(e);
                return;
            } catch (OpenR66ProtocolNotYetConnectionException e) {
                logger.debug("Not Yet Connected", e);
                exc = e;
                continue;
            }
        }
        if (exc != null) {
            taskRunner.setLocalChannelReference(new LocalChannelReference());
            logger.debug("Cannot Connect", exc);
            future.setResult(new R66Result(exc, null, true,
                    ErrorCode.ConnectionImpossible, taskRunner));
            // since no connection : just forget it
            if (nolog || taskRunner.shallIgnoreSave()) {
                try {
                    taskRunner.delete();
                } catch (WaarpDatabaseException e1) {
                }
            }
            future.setFailure(exc);
            return;
        }
    }

    public static void main(String[] args) {
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(DirectTransfer.class);
        }
        if (!getParams(args, false)) {
            logger.error(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
            if (!OutputFormat.isQuiet()) {
                System.out.println(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
            }
            if (DbConstant.admin != null && DbConstant.admin.isActive()) {
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(2);
        }
        long time1 = System.currentTimeMillis();
        R66Future future = new R66Future(true);

        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        try {
            DirectTransfer transaction = new DirectTransfer(future,
                    rhost, localFilename, rule, fileInfo, ismd5, block, idt,
                    networkTransaction);
            transaction.normalInfoAsWarn = snormalInfoAsWarn;
            logger.debug("rhost: " + rhost + ":" + transaction.remoteHost);
            transaction.run();
            future.awaitUninterruptibly();
            long time2 = System.currentTimeMillis();
            logger.debug("finish transfer: " + future.isSuccess());
            long delay = time2 - time1;
            R66Result result = future.getResult();
            OutputFormat outputFormat = new OutputFormat(DirectTransfer.class.getSimpleName(), args);
            if (future.isSuccess()) {
                if (result.getRunner().getErrorInfo() == ErrorCode.Warning) {
                    outputFormat.setValue(FIELDS.status.name(), 1);
                    outputFormat.setValue(FIELDS.statusTxt.name(),
                            Messages.getString("Transfer.Status") + Messages.getString("RequestInformation.Warned")); //$NON-NLS-1$
                } else {
                    outputFormat.setValue(FIELDS.status.name(), 0);
                    outputFormat.setValue(FIELDS.statusTxt.name(),
                            Messages.getString("Transfer.Status") + Messages.getString("RequestInformation.Success")); //$NON-NLS-1$
                }
                outputFormat.setValue(FIELDS.remote.name(), rhost);
                outputFormat.setValueString(result.getRunner().getJson());
                outputFormat.setValue("filefinal", (result.getFile() != null ? result.getFile().toString() : "no file"));
                outputFormat.setValue("delay", delay);
                if (transaction.normalInfoAsWarn) {
                    logger.warn(outputFormat.loggerOut());
                } else {
                    logger.info(outputFormat.loggerOut());
                }
                if (!OutputFormat.isQuiet()) {
                    outputFormat.sysout();
                }
                if (nolog || result.getRunner().shallIgnoreSave()) {
                    // In case of success, delete the runner
                    try {
                        result.getRunner().delete();
                    } catch (WaarpDatabaseException e) {
                        logger.warn("Cannot apply nolog to     " + result.getRunner().toShortString(),
                                e);
                    }
                }
                networkTransaction.closeAll();
                System.exit(0);
            } else {
                if (result == null || result.getRunner() == null) {
                    outputFormat.setValue(FIELDS.status.name(), 2);
                    outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("Transfer.FailedNoId")); //$NON-NLS-1$
                    outputFormat.setValue(FIELDS.remote.name(), rhost);
                    logger.error(outputFormat.loggerOut(), future.getCause());
                    outputFormat.setValue(FIELDS.error.name(), future.getCause().getMessage());
                    if (!OutputFormat.isQuiet()) {
                        outputFormat.sysout();
                    }
                    networkTransaction.closeAll();
                    System.exit(ErrorCode.Unknown.ordinal());
                }
                if (result.getRunner().getErrorInfo() == ErrorCode.Warning) {
                    outputFormat.setValue(FIELDS.status.name(), 1);
                    outputFormat.setValue(FIELDS.statusTxt.name(),
                            Messages.getString("Transfer.Status") + Messages.getString("RequestInformation.Warned")); //$NON-NLS-1$
                } else {
                    outputFormat.setValue(FIELDS.status.name(), 2);
                    outputFormat.setValue(FIELDS.statusTxt.name(),
                            Messages.getString("Transfer.Status") + Messages.getString("RequestInformation.Failure")); //$NON-NLS-1$
                }
                outputFormat.setValue(FIELDS.remote.name(), rhost);
                outputFormat.setValueString(result.getRunner().getJson());
                if (result.getRunner().getErrorInfo() == ErrorCode.Warning) {
                    logger.warn(outputFormat.loggerOut(), future.getCause());
                } else {
                    logger.error(outputFormat.loggerOut(), future.getCause());
                }
                outputFormat.setValue(FIELDS.error.name(), future.getCause().getMessage());
                if (!OutputFormat.isQuiet()) {
                    outputFormat.sysout();
                }
                networkTransaction.closeAll();
                System.exit(result.getCode().ordinal());
            }
        } catch (Throwable e) {
            logger.error("Exception", e);
        } finally {
            logger.debug("finish transfer: " + future.isDone() + ":" + future.isSuccess());
            networkTransaction.closeAll();
            // In case something wrong append
            if (future.isDone() && future.isSuccess()) {
                System.exit(0);
            } else {
                System.exit(66);
            }
        }
    }

}
