package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class Item {

    private static String name = "item";


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

        for(String line; (line = br.readLine()) != null; ) {
            String[] fields = line.split(",");
            if(fields.length != 9)
                continue;
            Put p = new Put(Bytes.toBytes(fields[0]));

            p.add(Bytes.toBytes("I"), Bytes.toBytes("ID"), Bytes.toBytes(fields[0])); // I_ID
            p.add(Bytes.toBytes("I"), Bytes.toBytes("IM_ID"), Bytes.toBytes(fields[1])); // I_IM_ID
            p.add(Bytes.toBytes("I"), Bytes.toBytes("NAME"), Bytes.toBytes(fields[2])); // I_NAME
            p.add(Bytes.toBytes("I"), Bytes.toBytes("PRICE"), Bytes.toBytes(fields[3])); // I_PRICE
            p.add(Bytes.toBytes("I"), Bytes.toBytes("DATA"), Bytes.toBytes(fields[4])); // I_DATA

            table.put(p);
        }


    }
}
