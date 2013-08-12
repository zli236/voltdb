package salebroker.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertIntoSellers extends VoltProcedure{
    public final SQLStmt insertStmt = new SQLStmt("insert into sellers values(?, ?, ?, ?, ?, ?);");

    public long run(int sellerId, String sellerName, String categoryId, String location, int zip, String contact) {
        voltQueueSQL(insertStmt, sellerId, sellerName, categoryId, location, zip, contact);
        return voltExecuteSQL()[0].getRowCount();
    }
}
