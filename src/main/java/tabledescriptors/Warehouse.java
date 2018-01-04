package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class Warehouse extends HTable {

    private static String name = "warehouse";

    public Warehouse(Configuration config, String name) throws IOException {
        super(config, name);
    }

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

            p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[1]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("STREET_1"), Bytes.toBytes(fields[1]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("STREET_2"), Bytes.toBytes(fields[2]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("CITY"), Bytes.toBytes(fields[3]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("STATE"), Bytes.toBytes(fields[4]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("ZIP"), Bytes.toBytes(fields[5]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("TAX"), Bytes.toBytes(fields[6]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("YTD"), Bytes.toBytes(fields[7]));

            table.put(p);
        }
        
        table.close();


    }
}
