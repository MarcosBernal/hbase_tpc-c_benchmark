package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class OrderLine {

    private static String name = "order_line";


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

            if(fields.length != 10){
               System.out.println("Found missing values in table: "+ name +" line: "+nline + " number of fields: " + fields.length);
                continue;
            }

            Put p = new Put(Bytes.toBytes(fields[2]+fields[1]+fields[0]+fields[3]));

            p.add(Bytes.toBytes("O"), Bytes.toBytes("ID"), Bytes.toBytes(fields[0])); // OL_O_ID
            p.add(Bytes.toBytes("D"), Bytes.toBytes("ID"), Bytes.toBytes(fields[1])); // OL_D_ID
            p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[2])); // OL_W_ID
            p.add(Bytes.toBytes("OL"), Bytes.toBytes("NUMBER"), Bytes.toBytes(fields[3])); // OL_NUMBER
            p.add(Bytes.toBytes("I"), Bytes.toBytes("ID"), Bytes.toBytes(fields[4])); // OL_I_ID
            // p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[5])); // OL_SUPPLY_W_ID REPEATED
            p.add(Bytes.toBytes("OL"), Bytes.toBytes("DELIVERY_D"), Bytes.toBytes(fields[6])); // OL_DELIVERY_D
            p.add(Bytes.toBytes("OL"), Bytes.toBytes("QUANTITY"), Bytes.toBytes(fields[7])); // OL_QUANTITY
            p.add(Bytes.toBytes("OL"), Bytes.toBytes("AMOUNT"), Bytes.toBytes(fields[8])); // OL_AMOUNT
            p.add(Bytes.toBytes("OL"), Bytes.toBytes("DIST_INFO"), Bytes.toBytes(fields[9]));  // OL_DIST_INFO

            table.put(p);
        }


    }
}
