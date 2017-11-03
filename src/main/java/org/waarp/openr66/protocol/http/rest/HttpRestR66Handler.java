/**
 * This file is part of Waarp Project (named also Waarp or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.http.rest;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.waarp.common.command.exception.Reply421Exception;
import org.waarp.common.command.exception.Reply530Exception;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.utility.WaarpNettyUtil;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.http.rest.handler.DbConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostAuthR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbRuleR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbTaskRunnerR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestBandwidthR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestBusinessR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestConfigR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestInformationR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestLogR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestServerR66Handler;
import org.waarp.openr66.protocol.http.rest.handler.HttpRestControlR66Handler;
import org.waarp.openr66.protocol.localhandler.ServerActions;

/**
 * Handler for Rest HTTP support for R66
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpRestR66Handler extends HttpRestHandler {

    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(HttpRestR66Handler.class);

    private static HashMap<String, DbSession> dbSessionFromUser = new HashMap<String, DbSession>();

    public static enum RESTHANDLERS {
        DbHostAuth(DbHostAuthR66RestMethodHandler.BASEURI, org.waarp.openr66.database.data.DbHostAuth.class),
        DbRule(DbRuleR66RestMethodHandler.BASEURI, org.waarp.openr66.database.data.DbRule.class),
        DbTaskRunner(DbTaskRunnerR66RestMethodHandler.BASEURI, org.waarp.openr66.database.data.DbTaskRunner.class),
        DbHostConfiguration(DbHostConfigurationR66RestMethodHandler.BASEURI,
                org.waarp.openr66.database.data.DbHostConfiguration.class),
        DbConfiguration(DbConfigurationR66RestMethodHandler.BASEURI,
                org.waarp.openr66.database.data.DbConfiguration.class),
        Bandwidth(HttpRestBandwidthR66Handler.BASEURI, null),
        Business(HttpRestBusinessR66Handler.BASEURI, null),
        Config(HttpRestConfigR66Handler.BASEURI, null),
        Information(HttpRestInformationR66Handler.BASEURI, null),
        Log(HttpRestLogR66Handler.BASEURI, null),
        Server(HttpRestServerR66Handler.BASEURI, null),
        Control(HttpRestControlR66Handler.BASEURI, null);

        public String uri;
        @SuppressWarnings("rawtypes")
        public Class clasz;

        @SuppressWarnings("rawtypes")
        RESTHANDLERS(String uri, Class clasz) {
            this.uri = uri;
            this.clasz = clasz;
        }

        public static RESTHANDLERS getRESTHANDLER(String baseUri) {
            for (RESTHANDLERS resthandler : RESTHANDLERS.values()) {
                if (resthandler.uri.equals(baseUri)) {
                    return resthandler;
                }
            }
            return null;
        }
    }

    /**
     * To be called once to ensure default is built
     */
    public static void defaultHandlers() {
        synchronized (defaultConfiguration) {
            if (defaultConfiguration.restHashMap.isEmpty()) {
                defaultConfiguration.REST_AUTHENTICATED = true;
                defaultConfiguration.RESTHANDLERS_CRUD = new byte[RESTHANDLERS.values().length];
                for (int i = 0; i < defaultConfiguration.RESTHANDLERS_CRUD.length; i++) {
                    defaultConfiguration.RESTHANDLERS_CRUD[i] = 0x0F;
                }
                METHOD[] methods = METHOD.values();
                defaultConfiguration.restHashMap.put(RESTHANDLERS.DbTaskRunner.uri,
                        new DbTaskRunnerR66RestMethodHandler(defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.DbHostAuth.uri, new DbHostAuthR66RestMethodHandler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.DbRule.uri, new DbRuleR66RestMethodHandler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.DbHostConfiguration.uri,
                        new DbHostConfigurationR66RestMethodHandler(defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.DbConfiguration.uri,
                        new DbConfigurationR66RestMethodHandler(defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Bandwidth.uri, new HttpRestBandwidthR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Business.uri, new HttpRestBusinessR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Config.uri, new HttpRestConfigR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Information.uri, new HttpRestInformationR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Log.uri, new HttpRestLogR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Server.uri, new HttpRestServerR66Handler(
                        defaultConfiguration, methods));
                defaultConfiguration.restHashMap.put(RESTHANDLERS.Control.uri, new HttpRestControlR66Handler(
                        defaultConfiguration, methods));
            }
        }
    }

    public HttpRestR66Handler(RestConfiguration config) {
        super(config);
        restHashMap = config.restHashMap;
    }

    protected static METHOD[] getMethods(byte check) {
        List<METHOD> methods = new ArrayList<METHOD>();
        if (RestConfiguration.CRUD.CREATE.isValid(check)) {
            methods.add(METHOD.POST);
        }
        if (RestConfiguration.CRUD.READ.isValid(check)) {
            methods.add(METHOD.GET);
        }
        if (RestConfiguration.CRUD.UPDATE.isValid(check)) {
            methods.add(METHOD.PUT);
        }
        if (RestConfiguration.CRUD.DELETE.isValid(check)) {
            methods.add(METHOD.DELETE);
        }
        return methods.toArray(new METHOD[0]);
    }

    public static void instantiateHandlers(RestConfiguration restConfiguration) {
        defaultHandlers();
        byte check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.DbTaskRunner.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.DbTaskRunner.uri, new DbTaskRunnerR66RestMethodHandler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.DbHostAuth.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.DbHostAuth.uri, new DbHostAuthR66RestMethodHandler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.DbRule.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.DbRule.uri, new DbRuleR66RestMethodHandler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.DbHostConfiguration.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.DbHostConfiguration.uri,
                    new DbHostConfigurationR66RestMethodHandler(restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.DbConfiguration.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.DbConfiguration.uri,
                    new DbConfigurationR66RestMethodHandler(restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Bandwidth.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Bandwidth.uri, new HttpRestBandwidthR66Handler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Business.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Business.uri, new HttpRestBusinessR66Handler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Config.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Config.uri, new HttpRestConfigR66Handler(restConfiguration,
                    methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Information.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Information.uri, new HttpRestInformationR66Handler(
                    restConfiguration, methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Log.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Log.uri, new HttpRestLogR66Handler(restConfiguration,
                    methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Server.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Server.uri, new HttpRestServerR66Handler(restConfiguration,
                    methods));
        }
        check = restConfiguration.RESTHANDLERS_CRUD[RESTHANDLERS.Control.ordinal()];
        if (check != 0) {
            METHOD[] methods = getMethods(check);
            restConfiguration.restHashMap.put(RESTHANDLERS.Control.uri, new HttpRestControlR66Handler(
                    restConfiguration, methods));
        }
        logger.debug("Initialized handler: " + RESTHANDLERS.values().length);
    }

    /**
     * Server Actions handler
     */
    private ServerActions serverHandler = new ServerActions();

    @Override
    protected void checkConnection(ChannelHandlerContext ctx) throws HttpInvalidAuthenticationException {
        logger.debug("Request: {} ### {}", arguments, response);
        String user = null;
        String key = null;
        if (restConfiguration.REST_AUTHENTICATED) {
            user = arguments.getXAuthUser();
            if (user == null || user.isEmpty()) {
                status = HttpResponseStatus.UNAUTHORIZED;
                throw new HttpInvalidAuthenticationException("Empty Authentication");
            }
            DbHostAuth host;
            try {
                host = new DbHostAuth(DbConstant.admin.getSession(), user);
                key = new String(host.getHostkey(), WaarpStringUtils.UTF8);
            } catch (WaarpDatabaseException e) {
                // might be global Admin
                if (user.equals(Configuration.configuration.getADMINNAME())) {
                    key = new String(Configuration.configuration.getSERVERADMINKEY(), WaarpStringUtils.UTF8);
                }
            }
            if (key == null || key.isEmpty()) {
                status = HttpResponseStatus.UNAUTHORIZED;
                throw new HttpInvalidAuthenticationException("Wrong Authentication");
            }
            if (restConfiguration.REST_SIGNATURE) {
                arguments.checkBaseAuthent(restConfiguration.hmacSha256, key, restConfiguration.REST_TIME_LIMIT);
            } else {
                arguments.checkTime(restConfiguration.REST_TIME_LIMIT);
            }
        } else {
            // User set only for right access, not for signature check
            user = Configuration.configuration.getADMINNAME();
            if (restConfiguration.REST_SIGNATURE) {
                arguments.checkBaseAuthent(restConfiguration.hmacSha256, null, restConfiguration.REST_TIME_LIMIT);
            } else {
                arguments.checkTime(restConfiguration.REST_TIME_LIMIT);
            }
        }
        getServerHandler().newSession();
        R66Session session = getServerHandler().getSession();
        if (!restConfiguration.REST_AUTHENTICATED) {
            // Default is Admin
            session.getAuth().specialNoSessionAuth(true, Configuration.configuration.getHOST_SSLID());
        } else {
            // we have one DbSession per connection, only after authentication
            DbSession temp = getDbSessionFromUser().get(user);
            if (temp == null) {
                try {
                    temp = new DbSession(DbConstant.admin, false);
                    getDbSessionFromUser().put(user, temp);
                } catch (WaarpDatabaseNoConnectionException e) {
                }
            }
            if (temp != null) {
                temp.useConnection();
                this.dbSession = temp;
            }
            try {
                session.getAuth().connectionHttps(getDbSession(), user,
                        FilesystemBasedDigest.passwdCrypt(key.getBytes(WaarpStringUtils.UTF8)));
            } catch (Reply530Exception e) {
                status = HttpResponseStatus.UNAUTHORIZED;
                throw new HttpInvalidAuthenticationException("Wrong Authentication", e);
            } catch (Reply421Exception e) {
                status = HttpResponseStatus.SERVICE_UNAVAILABLE;
                throw new HttpInvalidAuthenticationException("Service unavailable", e);
            }
        }
        arguments.setXAuthRole(session.getAuth().getRole());
        arguments.methodFromUri();
        arguments.methodFromHeader();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        getServerHandler().channelClosed(ctx);
    }

    /**
     * Called at the beginning of every new request
     * 
     * Override if needed
     */
    protected void initialize() {
        super.initialize();
    }

    /**
     * Initialize the REST service (server side) for one restConfiguration
     * 
     * @param restConfiguration
     */
    public static void initializeService(RestConfiguration restConfiguration) {
        instantiateHandlers(restConfiguration);
        if (group == null) {
            group = Configuration.configuration.getHttpChannelGroup();
        }
        // Configure the server.
        ServerBootstrap httpBootstrap = new ServerBootstrap();
        WaarpNettyUtil.setServerBootstrap(httpBootstrap, Configuration.configuration.getHttpBossGroup(),
                Configuration.configuration.getHttpWorkerGroup(), (int) Configuration.configuration.getTIMEOUTCON());
        // Set up the event pipeline factory.
        if (restConfiguration.REST_SSL) {
            httpBootstrap.childHandler(new HttpRestR66Initializer(false, Configuration.getWaarpSslContextFactory(),
                    restConfiguration));
        } else {
            httpBootstrap.childHandler(new HttpRestR66Initializer(false, null, restConfiguration));
        }
        // Bind and start to accept incoming connections.
        ChannelFuture future;
        if (restConfiguration != null && restConfiguration.REST_ADDRESS != null
                && !restConfiguration.REST_ADDRESS.isEmpty()) {
            future = httpBootstrap.bind(new InetSocketAddress(restConfiguration.REST_ADDRESS,
                    restConfiguration.REST_PORT));
        } else {
            future = httpBootstrap.bind(new InetSocketAddress(restConfiguration.REST_PORT));
        }
        try {
            future.await();
        } catch (InterruptedException e) {
        }
        if (future.isSuccess()) {
            group.add(future.channel());
        }
    }

    /**
     * @return the dbSessionFromUser
     */
    public static HashMap<String, DbSession> getDbSessionFromUser() {
        return dbSessionFromUser;
    }

    /**
     * @return the serverHandler
     */
    public ServerActions getServerHandler() {
        return serverHandler;
    }
}
