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
package org.waarp.openr66.database.data;

import java.sql.Types;
import java.util.concurrent.ConcurrentHashMap;

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Configuration Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbMultipleMonitor extends AbstractDbData {
    public static enum Columns {
        COUNTCONFIG,
        COUNTHOST,
        COUNTRULE,
        HOSTID
    }

    public static final int[] dbTypes = {
            Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.NVARCHAR };

    public static final String table = " MULTIPLEMONITOR ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbMultipleMonitor> dbR66MMHashMap =
            new ConcurrentHashMap<String, DbMultipleMonitor>();

    private String hostid;

    private int countConfig;

    private int countHost;

    private int countRule;

    // ALL TABLE SHOULD IMPLEMENT THIS
    public static final int NBPRKEY = 1;

    protected static final String selectAllFields = Columns.COUNTCONFIG
            .name()
            +
            ","
            +
            Columns.COUNTHOST
                    .name()
            +
            ","
            +
            Columns.COUNTRULE
                    .name()
            +
            ","
            +
            Columns.HOSTID
                    .name();

    protected static final String updateAllFields = Columns.COUNTCONFIG
            .name()
            +
            "=?,"
            +
            Columns.COUNTHOST
                    .name()
            +
            "=?,"
            +
            Columns.COUNTRULE
                    .name()
            +
            "=?";

    protected static final String insertAllValues = " (?,?,?,?) ";

    @Override
    protected void initObject() {
        primaryKey = new DbValue[] { new DbValue(hostid, Columns.HOSTID
                .name()) };
        otherFields = new DbValue[] {
                new DbValue(getCountConfig(), Columns.COUNTCONFIG.name()),
                new DbValue(getCountHost(), Columns.COUNTHOST.name()),
                new DbValue(getCountRule(), Columns.COUNTRULE.name()) };
        allFields = new DbValue[] {
                otherFields[0], otherFields[1], otherFields[2], primaryKey[0] };
    }

    @Override
    protected String getSelectAllFields() {
        return selectAllFields;
    }

    @Override
    protected String getTable() {
        return table;
    }

    @Override
    protected String getInsertAllValues() {
        return insertAllValues;
    }

    @Override
    protected String getUpdateAllFields() {
        return updateAllFields;
    }

    @Override
    protected void setToArray() {
        allFields[Columns.HOSTID.ordinal()].setValue(hostid);
        allFields[Columns.COUNTCONFIG.ordinal()].setValue(getCountConfig());
        allFields[Columns.COUNTHOST.ordinal()]
                .setValue(getCountHost());
        allFields[Columns.COUNTRULE.ordinal()]
                .setValue(getCountRule());
    }

    @Override
    protected void setFromArray() throws WaarpDatabaseSqlException {
        hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
        setCountConfig((Integer) allFields[Columns.COUNTCONFIG.ordinal()]
                .getValue());
        setCountHost((Integer) allFields[Columns.COUNTHOST.ordinal()]
                .getValue());
        setCountRule((Integer) allFields[Columns.COUNTRULE.ordinal()]
                .getValue());
    }

    @Override
    protected String getWherePrimaryKey() {
        return primaryKey[0].getColumn() + " = ? ";
    }

    @Override
    protected void setPrimaryKey() {
        primaryKey[0].setValue(hostid);
    }

    /**
     * @param dbSession
     * @param hostid
     * @param cc
     *            count for Config
     * @param ch
     *            count for Host
     * @param cr
     *            count for Rule
     */
    public DbMultipleMonitor(DbSession dbSession, String hostid, int cc, int ch, int cr) {
        super(dbSession);
        this.hostid = hostid;
        setCountConfig(cc);
        setCountHost(ch);
        setCountRule(cr);
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param hostid
     * @throws WaarpDatabaseException
     */
    public DbMultipleMonitor(DbSession dbSession, String hostid) throws WaarpDatabaseException {
        super(dbSession);
        this.hostid = hostid;
        // load from DB
        select();
    }

    @Override
    public void delete() throws WaarpDatabaseException {
        if (dbSession == null) {
            dbR66MMHashMap.remove(this.hostid);
            isSaved = false;
            return;
        }
        super.delete();
    }

    @Override
    public void insert() throws WaarpDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66MMHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
        super.insert();
    }

    @Override
    public boolean exist() throws WaarpDatabaseException {
        if (dbSession == null) {
            return dbR66MMHashMap.containsKey(hostid);
        }
        return super.exist();
    }

    @Override
    public void select() throws WaarpDatabaseException {
        if (dbSession == null) {
            DbMultipleMonitor conf = dbR66MMHashMap.get(this.hostid);
            if (conf == null) {
                throw new WaarpDatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++) {
                    allFields[i].setValue(conf.allFields[i].getValue());
                }
                setFromArray();
                isSaved = true;
                return;
            }
        }
        super.select();
    }

    @Override
    public void update() throws WaarpDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66MMHashMap.put(this.hostid, this);
            isSaved = true;
            return;
        }
        super.update();
    }

    /**
     * Private constructor for Commander only
     */
    private DbMultipleMonitor(DbSession session) {
        super(session);
    }

    /**
     * For instance from Commander when getting updated information
     * 
     * @param preparedStatement
     * @return the next updated Configuration
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbMultipleMonitor getFromStatement(DbPreparedStatement preparedStatement)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbMultipleMonitor dbMm = new DbMultipleMonitor(preparedStatement.getDbSession());
        dbMm.getValues(preparedStatement, dbMm.allFields);
        dbMm.setFromArray();
        dbMm.isSaved = true;
        return dbMm;
    }

    /**
     * 
     * @return the DbPreparedStatement for getting Updated Object in "FOR UPDATE" mode
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getUpdatedPrepareStament(DbSession session)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbMultipleMonitor multipleMonitor = new DbMultipleMonitor(session,
                Configuration.configuration.getHOST_ID(), 0, 0, 0);
        try {
            if (!multipleMonitor.exist()) {
                multipleMonitor.insert();
                session.commit();
            }
        } catch (WaarpDatabaseException e1) {
        }
        String request = "SELECT " + selectAllFields;
        request += " FROM " + table + " WHERE " + Columns.HOSTID.name() + " = '"
                + Configuration.configuration.getHOST_ID() + "'" +
                " FOR UPDATE ";
        DbPreparedStatement prep = new DbPreparedStatement(session, request);
        return prep;
    }

    /**
     * On Commander side
     * 
     * @return True if this is the last update
     */
    public boolean checkUpdateConfig() {
        if (getCountConfig() <= 0) {
            setCountConfig(Configuration.configuration.getMultipleMonitors());
            setCountConfig(getCountConfig() - 1);
            this.isSaved = false;
        } else {
            setCountConfig(getCountConfig() - 1);
            this.isSaved = false;
        }
        return this.getCountConfig() <= 0;
    }

    /**
     * On Commander side
     * 
     * @return True if this is the last update
     */
    public boolean checkUpdateHost() {
        if (getCountHost() <= 0) {
            setCountHost(Configuration.configuration.getMultipleMonitors());
            setCountHost(getCountHost() - 1);
            this.isSaved = false;
        } else {
            setCountHost(getCountHost() - 1);
            this.isSaved = false;
        }
        return this.getCountHost() <= 0;
    }

    /**
     * On Commander side
     * 
     * @return True if this is the last update
     */
    public boolean checkUpdateRule() {
        if (getCountRule() <= 0) {
            setCountRule(Configuration.configuration.getMultipleMonitors());
            setCountRule(getCountRule() - 1);
            this.isSaved = false;
        } else {
            setCountRule(getCountRule() - 1);
            this.isSaved = false;
        }
        return this.getCountRule() <= 0;
    }

    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
    }

    /**
     * return the String representation
     */
    public String toString() {
        return "DbMM " + getCountConfig() + ":" + getCountHost() + ":" + getCountRule();
    }

    /**
     * @return the countConfig
     */
    public int getCountConfig() {
        return countConfig;
    }

    /**
     * @param countConfig the countConfig to set
     */
    private void setCountConfig(int countConfig) {
        this.countConfig = countConfig;
    }

    /**
     * @return the countHost
     */
    public int getCountHost() {
        return countHost;
    }

    /**
     * @param countHost the countHost to set
     */
    private void setCountHost(int countHost) {
        this.countHost = countHost;
    }

    /**
     * @return the countRule
     */
    public int getCountRule() {
        return countRule;
    }

    /**
     * @param countRule the countRule to set
     */
    private void setCountRule(int countRule) {
        this.countRule = countRule;
    }
}
