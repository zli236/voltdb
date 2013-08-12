package salebroker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;



public class produceCSV {
    private static int itemNum = 1000000;
    private static int itemIdRange = 100000;
    private static int sellerNum = 20000;
    private static int categoryIdNum = 500;

    public static void produceItems(String csvDir) {
        try {
            File file = new File(csvDir+"/items.csv");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            int itemId;
            String itemName;
            int sellerId;
            float price;
            int categoryId;
            Random rand = new Random(System.currentTimeMillis());
            String content = "";
            for(int i = 0; i < itemNum; i++) {
                itemId = rand.nextInt(itemIdRange);
                itemName = "Dummy Item "+itemId;
                sellerId = rand.nextInt(sellerNum);
                price = rand.nextFloat() * 1000;
                categoryId = rand.nextInt(categoryIdNum);
                content = new StringBuilder()
		    .append(itemId).append(',')
		    .append(itemName).append(',')
		    .append(sellerId).append(',')
		    .append(price).append(',')
		    .append(categoryId)
		    .append('\n')
		    .toString();
                bw.write(content);
            }
            bw.close();
            System.out.println("Done items");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void produceSellers(String csvDir) {
        try {
            File file = new File(csvDir+"/sellers.csv");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            int sellerId;
            String sellerName;
            int categoryId;
            String location;
            int zip;
            String contact = "";

            Random rand = new Random(System.currentTimeMillis());
            for( int i = 0; i < sellerNum; i++) {
                sellerId = i;
                sellerName = "Dummy Seller"+sellerId;
                categoryId = rand.nextInt(categoryIdNum);
                location = "Dummy Location"+sellerId;
                zip = rand.nextInt(100000);
                contact = "Dummy"+sellerId+"@Seller.com";
                bw.write(new StringBuilder().append(sellerId).append(',')
			 .append(sellerName).append(',')
			 .append(categoryId).append(',')
			 .append(location).append(',')
			 .append(zip).append(',')
			 .append(contact).append('\n')
			 .toString());
            }
            bw.close();
            System.out.println("Done sellers");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void produceCategories(String csvDir) {
        try {
            File file = new File(csvDir+"/categories.csv");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            int categoryId;
            String categoryName;

            Random rand = new Random(System.currentTimeMillis());
            for( int i = 0; i < categoryIdNum; i++) {
                categoryId = i;
                categoryName = "Dummy Category "+categoryId;
                bw.write(new StringBuilder().append(categoryId).append(',')
			 .append(categoryName).append('\n')
			 .toString());
            }
            bw.close();
            System.out.println("Done categories");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) throws Exception
    {
        produceCategories(args[0]);
        produceSellers(args[0]);
        produceItems(args[0]);
    }
}
