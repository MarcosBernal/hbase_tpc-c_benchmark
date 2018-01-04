package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class Stock {

    private static String name = "stock";


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

            if(fields.length != 17){
               System.out.println("Found missing values in table: "+ name +" line: "+nline + " number of fields: " + fields.length);
                continue;
            }

            Put p = new Put(Bytes.toBytes(fields[1]+fields[0]));

            p.add(Bytes.toBytes("I"), Bytes.toBytes("ID"), Bytes.toBytes(fields[0])); // S_I_ID
            p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[1])); // S_W_ID
            p.add(Bytes.toBytes("S"), Bytes.toBytes("QUANTITY"), Bytes.toBytes(fields[2])); // S_QUANTITY
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_01"), Bytes.toBytes(fields[3])); // S_DISTI_01
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_02"), Bytes.toBytes(fields[4])); // S_DISTI_02
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_03"), Bytes.toBytes(fields[5])); // S_DISTI_03
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_04"), Bytes.toBytes(fields[6])); // S_DISTI_04
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_05"), Bytes.toBytes(fields[7])); // S_DISTI_05
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_06"), Bytes.toBytes(fields[8])); // S_DISTI_06
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_07"), Bytes.toBytes(fields[9])); // S_DISTI_07
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_08"), Bytes.toBytes(fields[10])); // S_DISTI_08
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_09"), Bytes.toBytes(fields[11])); // S_DISTI_09
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DIST_10"), Bytes.toBytes(fields[12])); // S_DISTI_10
            p.add(Bytes.toBytes("S"), Bytes.toBytes("YTD"), Bytes.toBytes(fields[13])); // s_DISTI_PRICE
            p.add(Bytes.toBytes("S"), Bytes.toBytes("ORDER_CNT"), Bytes.toBytes(fields[14])); // s_DISTI_PRICE
            p.add(Bytes.toBytes("S"), Bytes.toBytes("REMOTE_CNT"), Bytes.toBytes(fields[15])); // I_PRICE
            p.add(Bytes.toBytes("S"), Bytes.toBytes("DATA"), Bytes.toBytes(fields[16])); // I_PRICE

            table.put(p);
        }

        table.close();

    }
}
