package tabledescriptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Order {

    private static String name = "orders";


    public static HTableDescriptor createDescriptor(){
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(name)));
        descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));
        return descriptor;
    }
    
    static DateFormat timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static long getTimeLong(String date) {
    	try {
			return timeParser.parse(date.split("\\.")[0]).getTime();
		} catch (ParseException e) { e.printStackTrace(); }
    	return 0;
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

            if(fields.length != 8){
               System.out.println("Found missing values in table: "+ name +" line: "+nline + " number of fields: " + fields.length);
                continue;
            }

            Put p = new Put(Bytes.toBytes(fields[2]+fields[1]+fields[0]));

            p.add(Bytes.toBytes("O"), Bytes.toBytes("ID"), Bytes.toBytes(fields[0]));
            p.add(Bytes.toBytes("D"), Bytes.toBytes("ID"), Bytes.toBytes(fields[1]));
            p.add(Bytes.toBytes("W"), Bytes.toBytes("ID"), Bytes.toBytes(fields[2]));
            p.add(Bytes.toBytes("C"), Bytes.toBytes("ID"), Bytes.toBytes(fields[3]));
            p.add(Bytes.toBytes("O"), Bytes.toBytes("ENTRY_D"), Bytes.toBytes(Order.getTimeLong(fields[4])));
            p.add(Bytes.toBytes("O"), Bytes.toBytes("CARRIER_ID"), Bytes.toBytes(fields[5]));
            p.add(Bytes.toBytes("O"), Bytes.toBytes("OL_CNT"), Bytes.toBytes(fields[6]));
            p.add(Bytes.toBytes("O"), Bytes.toBytes("ALL_LOCAL"), Bytes.toBytes(fields[7]));


            table.put(p);
        }

        table.close();

    }
}
