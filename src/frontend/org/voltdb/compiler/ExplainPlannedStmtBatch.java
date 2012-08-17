/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.VoltDB;

/**
 * Holds a batch of planned SQL statements exclusively for @Explain.
 * DO not inherit from AdhocPlannedStmtBatch for efficiency reason( if else check in ClientInterface.processFinishedCompilerWork() ).
 * Therefore there's this code redundancy
 */
public class ExplainPlannedStmtBatch extends AsyncCompilerResult implements Cloneable {

    /**
     *
     */
    private static final long serialVersionUID = 4333120982651893011L;

    // not persisted across serializations
    public final String sqlBatchText;

    // May be reassigned if the planner infers single partition work.
    // Also not persisted across serializations
    public Object partitionParam;

    // The planned statements.
    // Do not add statements directly. Use addStatement so that the readOnly flag
    // is updated
    public final List<AdHocPlannedStatement> plannedStatements = new ArrayList<AdHocPlannedStatement>();

    // Assume the batch is read-only until we see the first non-select statement.
    private boolean readOnly = true;

    /**
     * Statement batch constructor.
     *
     * IMPORTANT: sqlBatchText is not maintained or updated by this class when
     * statements are added. The caller is responsible for splitting the batch
     * text and assuring that the individual SQL statements correspond to the
     * original.
     *
     * @param sqlBatchText     Un-split SQL for the entire batch
     * @param partitionParam   Optional partition parameter or null
     * @param catalogVersion   Catalog version number
     * @param clientHandle     Client handle
     * @param connectionId     Connection ID
     * @param hostname         Host name
     * @param adminConnection  True if an admin connection
     * @param clientData       Optional client data object or null
     */
    public ExplainPlannedStmtBatch(
            String sqlBatchText,
            Object partitionParam,
            long clientHandle,
            long connectionId,
            String hostname,
            boolean adminConnection,
            Object clientData) {
        this.sqlBatchText = sqlBatchText;
        this.partitionParam = partitionParam;
        this.clientHandle = clientHandle;
        this.connectionId = connectionId;
        this.hostname = hostname;
        this.adminConnection = adminConnection;
        this.clientData = clientData;
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        retval += "\n  sql: " + ((sqlBatchText != null) ? sqlBatchText : "null");
        return retval;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve all the SQL statement text as a list of strings.
     *
     * @return list of SQL statement strings
     */
    public List<String> getSQLStatements() {
        List<String> sqlStatements = new ArrayList<String>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            sqlStatements.add(new String(plannedStatement.sql, VoltDB.UTF8ENCODING));
        }
        return sqlStatements;
    }

    /**
     * Add an AdHocPlannedStatement to this batch.
     */
    public void addStatement(AdHocPlannedStatement plannedStmt) {
        // The first non-select statement makes it not read-only.
        if (!plannedStmt.readOnly) {
            readOnly = false;
        }
        plannedStatements.add(plannedStmt);
    }

    /**
     * Detect if batch is compatible with single partition optimizations
     * @return true if nothing is replicated and nothing has a collector.
     */
    public boolean isSinglePartitionCompatible() {
        for (AdHocPlannedStatement plannedStmt : plannedStatements) {
            if (plannedStmt.collectorFragment != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the number of planned statements.
     *
     * @return planned statement count
     */
    public int getPlannedStatementCount() {
        return plannedStatements.size();
    }

    /**
     * Get a particular planned statement by index.
     * The index is not validated here.
     *
     * @param index
     * @return planned statement
     */
    public AdHocPlannedStatement getPlannedStatement(int index) {
        return plannedStatements.get(index);
    }

    /**
     * Read-only flag accessor
     *
     * @return true if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    public int getPlanArraySerializedSize() {
        int size = 2; // sizeof batch
        for (AdHocPlannedStatement cs : plannedStatements) {
            size += cs.getSerializedSize();
        }
        return size;
    }

    public void flattenPlanArrayToBuffer(ByteBuffer buf) throws IOException {
        buf.putShort((short) plannedStatements.size());
        for (AdHocPlannedStatement cs : plannedStatements) {
            cs.flattenToBuffer(buf);
        }
    }

    public static AdHocPlannedStatement[] planArrayFromBuffer(ByteBuffer buf) throws IOException {
        short csCount = buf.getShort();
        AdHocPlannedStatement[] statements = new AdHocPlannedStatement[csCount];
        for (int i = 0; i < csCount; ++i) {
            AdHocPlannedStatement cs = AdHocPlannedStatement.fromBuffer(buf);
            statements[i] = cs;
        }
        return statements;
    }
}