package salebroker.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class deleteFromItems extends VoltProcedure{
    public final SQLStmt deleteStmt = new SQLStmt("delete from items where seller_id = ? and item_id = ?;");

    public long run(int serllerId, int itemId) {
        voltQueueSQL(deleteStmt, serllerId, itemId);
        return voltExecuteSQL()[0].getRowCount();
    }
}