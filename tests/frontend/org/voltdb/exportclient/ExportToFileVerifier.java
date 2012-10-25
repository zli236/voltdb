/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltdb.exportclient;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.export.ExportTestVerifier;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class ExportToFileVerifier {
    // hash table name + partition to verifier
    public final HashMap<String, ExportTestVerifier> m_verifiers =
        new HashMap<String, ExportTestVerifier>();

    private final File m_paths[];
    private final String m_nonce;

    public ExportToFileVerifier(File paths[], String nonce) {
        m_paths = paths;
        m_nonce = nonce;
    }

    public void addRow(String tableName, Object partitionHash, Object[] data)
    {
        int partition = TheHashinator.hashToPartition(partitionHash);
        ExportTestVerifier verifier = m_verifiers.get(tableName + partition);
        if (verifier == null)
        {
            verifier = new ExportTestVerifier();
            m_verifiers.put(tableName + partition, verifier);
        }
        //verifier.addRow(data);
    }

    public boolean verifyRows() throws Exception {
        TreeMap<Long, List<File>> generations = new TreeMap<Long, List<File>>();
        /*
         * Get all the files for each generation so we process them in the right order
         */
        for (File f : m_paths) {
            for (File f2 : f.listFiles()) {
                if (f2.getName().endsWith("csv")) {
                    Long generation;
                    if (f2.getName().startsWith("active")) {
                        generation = Long.valueOf(f2.getName().split("-")[2]);
                    } else {
                        generation = Long.valueOf(f2.getName().split("-")[1]);
                    }
                    if (!generations.containsKey(generation)) generations.put(generation, new ArrayList<File>());
                    List<File> generationFiles = generations.get(generation);
                    generationFiles.add(f2);

                }
            }
        }

        /*
         * Process the row data in each file
         */
        for (List<File> generationFiles : generations.values()) {
            for (File f : generationFiles) {
                String tableName;
                if (f.getName().startsWith("active")) {
                    tableName = f.getName().split("-")[1];
                } else {
                    tableName = f.getName().split("-")[0];
                }


                FileInputStream fis = new FileInputStream(f);
                BufferedInputStream bis = new BufferedInputStream(fis);
                InputStreamReader isr = new InputStreamReader(bis, VoltDB.UTF8ENCODING);
                CSVReader csvreader = new CSVReader(isr);
                String next[] = null;
                while ((next = csvreader.readNext()) != null) {
                    final int partitionId = Integer.valueOf(next[3]);
                    StringBuilder sb = new StringBuilder();
                    for (String s : next) {
                        sb.append(s).append(", ");
                    }
                    System.out.println(sb);
                    m_verifiers.get(tableName + partitionId).processRow(next);
                }
            }
        }

        return false;
    }
}
