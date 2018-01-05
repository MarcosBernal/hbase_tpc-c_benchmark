package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class Customer {

    private static String name = "customer";


    public static HTableDescriptor createDescriptor(){
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(name)));
        descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));
        return descriptor;
    }

    public static void insertData(Configuration config, String folderpath) throws IOException {
        File file_path = new File(folderpath+"/"+name+".csv");
        BufferedReader br = null;

        if(!file_path.isFile()) {
            System.out.println("File"+ folderpath+"/"+name+".csv NOT found");
            return;
        }
        try {
            br = new BufferedReader(new FileReader(file_path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        HTable table = new HTable(config, name);
	      long nline = 0;

        for(String line; (line = br.readLine()) != null; ) {
            String[] fields = line.split(",");
	          nline++;

            if(fields.length != 21){
								System.out.println("Found missing values in line: "+nline + " number of fields: " + fields.length);
                continue;
						}

            Put p = new Put(Bytes.toBytes(fields[2]+fields[1]+fields[0]));

            p.add(Bytes.toBytes("C"), Bytes.toBytes("ID"), Bytes.toBytes(fields[0]));
            p.add(Bytes.toBytes("D"), Bytes.toBytes("ID"), Bytes.toBytes(fields[1]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[2]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("FIRST"), Bytes.toBytes(fields[3]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("MIDDLE"), Bytes.toBytes(fields[4]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("LAST"), Bytes.toBytes(fields[5]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("STREET_1"), Bytes.toBytes(fields[6]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("STREET_2"), Bytes.toBytes(fields[7]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("CITY"), Bytes.toBytes(fields[8]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("STATE"), Bytes.toBytes(fields[9]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("ZIP"), Bytes.toBytes(fields[10]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("PHONE"), Bytes.toBytes(fields[11]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("SINCE"), Bytes.toBytes(fields[12]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("CREDIT"), Bytes.toBytes(fields[13]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("CREDIT_LIM"), Bytes.toBytes(fields[14]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("DISCOUNT"), Bytes.toBytes(fields[15]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("BALANCE"), Bytes.toBytes(fields[16]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("YTD_PAYMENT"), Bytes.toBytes(fields[17]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("PAYMENT_CNT"), Bytes.toBytes(fields[18]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("DELIVERY_CNT"), Bytes.toBytes(fields[19]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("DATA"), Bytes.toBytes(fields[20]));

            table.put(p);
        }
        
        table.close();


    }
}
