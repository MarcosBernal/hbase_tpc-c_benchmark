import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;


public class HBaseTPCC {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseTPCC(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTPCCTables() throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
    }

    public void loadTables(String folder)throws IOException{
        //TO IMPLEMENT
        System.exit(-1);
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }



    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;

    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public int query4(String warehouseId, String[] districtIds) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return 0;
    }

    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTables, loadTables, query1, query2, query3, query4], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTables: csvsFolder.\n " +
                    "\tb) If query1: warehouseId districtId startDate endData.\n  " +
                    "\tc) If query2: warehouseId districtId customerId listOfDiscounts.\n  " +
                    "\td) If query3: warehouseId districtId customerId.\n  " +
                    "\te) If query4: warehouseId listOfdistrictId.\n  ");
            System.exit(-1);
        }
        HBaseTPCC hBaseTPCC = new HBaseTPCC(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLES")){
            hBaseTPCC.createTPCCTables();
        }
        else if(args[1].toUpperCase().equals("LOADTABLES")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseTPCC.loadTables(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) warehouseId 4) districtId 5) startDate 6) endData");
                System.exit(-1);
            }

            List<String> customerIds = hBaseTPCC.query1(args[2], args[3], args[4], args[5]);
            System.out.println("There are "+customerIds.size()+" customers that order products from warehouse "+args[2]+" of district "+args[3]+" during after the "+args[4]+" and before the "+args[5]+".");
            System.out.println("The list of customers is: "+Arrays.toString(customerIds.toArray(new String[customerIds.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) warehouseId 4) districtId 5) customerId 6) listOfDiscounts");
                System.exit(-1);
            }
            hBaseTPCC.query2(args[2], args[3], args[4], args[5].split(","));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=5){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) districtId 5) customerId");
                System.exit(-1);
            }
            String[] discounts = hBaseTPCC.query3(args[2], args[3], args[4]);
            System.out.println("The last 4 discounts obtained from Customer "+args[4]+" of warehouse "+args[2]+" of district "+args[3]+" are: "+Arrays.toString(discounts));
        }
        else if(args[1].toUpperCase().equals("QUERY4")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) listOfDistrictIds");
                System.exit(-1);
            }
            System.out.println("There are "+hBaseTPCC.query4(args[2], args[3].split(","))+" customers that belong to warehouse "+args[2]+" of districts "+args[3]+".");
        }
        else{
            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables, query1, query2, query3, query4], 3)Extra parameters for loadTables and queries:" +
                    "a) If loadTables: csvsFolder." +
                    "b) If query1: warehouseId districtId startDate endData" +
                    "c) If query2: warehouseId districtId customerId listOfDiscounts" +
                    "d) If query3: warehouseId districtId customerId " +
                    "e) If query4: warehouseId listOfdistrictId");
            System.exit(-1);
        }

    }



}
