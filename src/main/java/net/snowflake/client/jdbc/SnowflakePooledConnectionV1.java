/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Snowflake pooled connection implementation
 *
 * @author dmay
 */
public class SnowflakePooledConnectionV1 implements PooledConnection {
    private SnowflakeConnectionV1 connection;
    private List<ConnectionEventListener> connectionEventListeners;
    private List<StatementEventListener> statementEventListeners;

    public SnowflakePooledConnectionV1(SnowflakeConnectionV1 connection) {
        this.connection = connection;
        connection.pooledConnection = this;
        this.statementEventListeners = new CopyOnWriteArrayList<>();
        this.connectionEventListeners = new CopyOnWriteArrayList<>();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public void close() throws SQLException {
        this.connection.pooledConnection = null;
        this.connection.close();
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        this.connectionEventListeners.add(listener);
    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
        this.connectionEventListeners.remove(listener);
    }

    public void addStatementEventListener(StatementEventListener listener) {
        this.statementEventListeners.add(listener);
    }

    public void removeStatementEventListener(StatementEventListener listener) {
        this.statementEventListeners.remove(listener);
    }

    public void fireStatementClosed(Statement st) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st);

            for (StatementEventListener listener : this.statementEventListeners) {
                listener.statementClosed(event);
            }
        }
    }

    public void fireStatementErrorOccured(Statement st, SQLException ex) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st, ex);

            for (StatementEventListener listener : this.statementEventListeners) {
                listener.statementErrorOccurred(event);
            }
        }
    }

    public void fireConnectionClosed() {
        ConnectionEvent event = new ConnectionEvent(this);

        for (ConnectionEventListener listener : this.connectionEventListeners) {
            listener.connectionClosed(event);
        }
    }

    public void fireConnectionErrorOccured(SQLException ex) {
        ConnectionEvent event = new ConnectionEvent(this, ex);

        for (ConnectionEventListener listener : this.connectionEventListeners) {
            listener.connectionErrorOccurred(event);
        }
    }
}