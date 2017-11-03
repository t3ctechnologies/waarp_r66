/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.http.rest.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler.COMMAND_TYPE;
import org.waarp.gateway.kernel.rest.HttpRestHandler.METHOD;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.json.BandwidthJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Bandwidth Http REST interface: http://host/bandwidth?... + BandwidthJsonPacket as GET or PUT
 * 
 * @author "Frederic Bregier"
 *
 */
public class HttpRestBandwidthR66Handler extends HttpRestAbstractR66Handler {

    public static final String BASEURI = "bandwidth";
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(HttpRestBandwidthR66Handler.class);

    public HttpRestBandwidthR66Handler(RestConfiguration config, METHOD... methods) {
        super(BASEURI, config, METHOD.OPTIONS);
        setIntersectionMethods(methods, METHOD.GET, METHOD.PUT);
    }

    @Override
    public void endParsingRequest(HttpRestHandler handler, RestArgument arguments, RestArgument result, Object body)
            throws HttpIncorrectRequestException, HttpInvalidAuthenticationException {
        logger.debug("debug: {} ### {}", arguments, result);
        if (body != null) {
            logger.debug("Obj: {}", body);
        }
        handler.setWillClose(false);
        ServerActions serverHandler = ((HttpRestR66Handler) handler).getServerHandler();
        // now action according to body
        JsonPacket json = (JsonPacket) body;
        if (json == null) {
            result.setDetail("not enough information");
            setError(handler, result, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        result.getAnswer().put(AbstractDbData.JSON_MODEL, RESTHANDLERS.Bandwidth.name());
        try {
            if (json instanceof BandwidthJsonPacket) {//
                // setter, writeglobal, readglobal, writesession, readsession
                BandwidthJsonPacket node = (BandwidthJsonPacket) json;
                boolean setter = node.isSetter();
                if (setter && arguments.getMethod() != METHOD.PUT) {
                    // wrong
                    result.setDetail("Setter should be requested with a PUT method");
                    setError(handler, result, HttpResponseStatus.CONFLICT);
                    return;
                } else if (!setter && arguments.getMethod() != METHOD.GET) {
                    // wrong
                    result.setDetail("Getter should not be requested with a GET method");
                    setError(handler, result, HttpResponseStatus.CONFLICT);
                    return;
                }
                if (setter) {
                    result.setCommand(ACTIONS_TYPE.SetBandwidth.name());
                } else {
                    result.setCommand(ACTIONS_TYPE.GetBandwidth.name());
                }
                // request of current values or set new values
                long[] lresult = serverHandler.bandwidth(setter,
                        node.getWriteglobal(), node.getReadglobal(),
                        node.getWritesession(), node.getReadsession());
                // Now answer
                node.setWriteglobal(lresult[0]);
                node.setReadglobal(lresult[1]);
                node.setWritesession(lresult[2]);
                node.setReadsession(lresult[3]);
                setOk(handler, result, json, HttpResponseStatus.OK);
            } else {
                logger.info("Validation is ignored: " + json);
                result.setDetail("Unknown command");
                setError(handler, result, json, HttpResponseStatus.PRECONDITION_FAILED);
            }
        } catch (OpenR66ProtocolNotAuthenticatedException e) {
            throw new HttpInvalidAuthenticationException(e);
        }
    }

    protected ArrayNode getDetailedAllow() {
        ArrayNode node = JsonHandler.createArrayNode();

        BandwidthJsonPacket node3 = new BandwidthJsonPacket();
        node3.setComment("Bandwidth getter (GET)");
        node3.setRequestUserPacket();
        ObjectNode node2;
        ArrayNode node1 = JsonHandler.createArrayNode();
        try {
            node1.add(node3.createObjectNode());
            if (this.methods.contains(METHOD.GET)) {
                node2 = RestArgument.fillDetailedAllow(METHOD.GET, this.path, ACTIONS_TYPE.GetBandwidth.name(),
                        node3.createObjectNode(), node1);
                node.add(node2);
            }
        } catch (OpenR66ProtocolPacketException e1) {
        }

        if (this.methods.contains(METHOD.PUT)) {
            node3.setComment("Bandwidth setter (PUT)");
            try {
                node2 = RestArgument.fillDetailedAllow(METHOD.PUT, this.path, ACTIONS_TYPE.SetBandwidth.name(),
                        node3.createObjectNode(), node1);
                node.add(node2);
            } catch (OpenR66ProtocolPacketException e1) {
            }
        }
        node2 = RestArgument.fillDetailedAllow(METHOD.OPTIONS, this.path, COMMAND_TYPE.OPTIONS.name(), null, null);
        node.add(node2);

        return node;
    }
}
