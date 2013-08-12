/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package salebroker.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

//@ProcInfo (
//        partitionInfo = "items.item_id:0",
//        singlePartition = true
//        )

public class FindNearerSellersByItemName extends VoltProcedure{
    public final SQLStmt findPotentialSellersStmt = new SQLStmt("SELECT seller_id, zip, contact from sellers, items where "
            + "sellers.seller_id = items.seller_id and items.item_name like '%?%';");

    public VoltTable run(String itemName, String categoryId, int customerZip, int myZip) {

        voltQueueSQL(findPotentialSellersStmt, itemName);
        VoltTable results = voltExecuteSQL()[0];

        if (results.getRowCount() == 0) {
            return null;
        }

        else {
            ColumnInfo[] cInfoArray = new ColumnInfo[results.getColumnCount()];
            for( int i = 0; i < results.getColumnCount(); i++) {
                cInfoArray[i] = new ColumnInfo( results.getColumnName(i),results.getColumnType(i) );
            }

            //only keep the sellers has shipping cost to the customer cheaper than mine
            //need to substitute with a better shipping cost function
            VoltTable finalResults = new VoltTable(cInfoArray);
            while(results.advanceRow()) {
                int zip = (Integer)results.get(1, VoltType.INTEGER);
                if(Math.abs(zip-customerZip) < Math.abs(myZip-customerZip)) {
                    finalResults.add(results.cloneRow());
                }
            }
            return finalResults;
        }
    }
}
