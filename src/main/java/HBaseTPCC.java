import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import tabledescriptors.*;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

        // Create a table for warehouse
        HTableDescriptor d_warehouse = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("warehouse")));
        HTableDescriptor d_stock = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("stock")));
        HTableDescriptor d_item = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("item")));
        HTableDescriptor d_history = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("history")));
        HTableDescriptor d_new_order = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("new_order")));
        HTableDescriptor d_order_line = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("order_line")));
        HTableDescriptor d_district = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("district")));
        HTableDescriptor d_customer = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("customer")));
        HTableDescriptor d_order = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("orders")));


        // Relationship defined in pages 11-17 of http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
        // Each letter refer to a table(acronym)
        d_warehouse.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));

        d_district.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
        d_district.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));

        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("C"));
        columnDescriptor.setMaxVersions(6); // Due to requirements of query 2
        d_customer.addFamily(columnDescriptor);
        
        d_customer.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
	    d_customer.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));

        d_history.addFamily(new HColumnDescriptor(Bytes.toBytes("H")));
        d_history.addFamily(new HColumnDescriptor(Bytes.toBytes("C")));
        d_history.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
	    d_history.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));

        d_new_order.addFamily(new HColumnDescriptor(Bytes.toBytes("O")));
        d_new_order.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
        d_new_order.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));

        d_order.addFamily(new HColumnDescriptor(Bytes.toBytes("O")));
      	d_order.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
      	d_order.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));
        d_order.addFamily(new HColumnDescriptor(Bytes.toBytes("C")));

        d_order_line.addFamily(new HColumnDescriptor(Bytes.toBytes("O")));
        d_order_line.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
        d_order_line.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));
        d_order_line.addFamily(new HColumnDescriptor(Bytes.toBytes("OL")));
        d_order_line.addFamily(new HColumnDescriptor(Bytes.toBytes("I")));

        d_item.addFamily(new HColumnDescriptor(Bytes.toBytes("I")));

        d_stock.addFamily(new HColumnDescriptor(Bytes.toBytes("I")));
        d_stock.addFamily(new HColumnDescriptor(Bytes.toBytes("W")));
        d_stock.addFamily(new HColumnDescriptor(Bytes.toBytes("S")));


        this.hBaseAdmin.createTable(d_warehouse);
        this.hBaseAdmin.createTable(d_stock);
        this.hBaseAdmin.createTable(d_item);
        this.hBaseAdmin.createTable(d_history);
        this.hBaseAdmin.createTable(d_new_order);
        this.hBaseAdmin.createTable(d_order_line);
        this.hBaseAdmin.createTable(d_district);
        this.hBaseAdmin.createTable(d_customer);
        this.hBaseAdmin.createTable(d_order);

    }

    public void loadTables(String folderpath)throws IOException{
        //File folder = new File(folderpath);
        //File[] listOfFiles = folder.listFiles();

        Warehouse.insertData(config, folderpath);
        District.insertData(config, folderpath);
        Customer.insertData(config, folderpath);
        History.insertData(config, folderpath);
        NewOrder.insertData(config, folderpath);
        Order.insertData(config, folderpath);
        OrderLine.insertData(config, folderpath);
        Item.insertData(config, folderpath);
        Stock.insertData(config, folderpath);

    }

    public void dropTPCCTables() throws IOException{
        
        this.hBaseAdmin.disableTable("warehouse");
        this.hBaseAdmin.deleteTable("warehouse");

        this.hBaseAdmin.disableTable("stock");
        this.hBaseAdmin.deleteTable("stock");

        this.hBaseAdmin.disableTable("item");
        this.hBaseAdmin.deleteTable("item");

        this.hBaseAdmin.disableTable("history");
        this.hBaseAdmin.deleteTable("history");

        this.hBaseAdmin.disableTable("new_order");
        this.hBaseAdmin.deleteTable("new_order");

        this.hBaseAdmin.disableTable("order_line");
        this.hBaseAdmin.deleteTable("order_line");

        this.hBaseAdmin.disableTable("district");
        this.hBaseAdmin.deleteTable("district");

        this.hBaseAdmin.disableTable("customer");
        this.hBaseAdmin.deleteTable("customer");

        this.hBaseAdmin.disableTable("orders");
        this.hBaseAdmin.deleteTable("orders");
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
    	
    	DateFormat timeParser = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SSS");
        long startDateLong = 0;
        long endDateLong = 0;
        HTable table = new HTable(config, "orders");
        List<String> customers = new ArrayList<String>();
        
    	try {
			startDateLong = timeParser.parse(startDate).getTime();
			endDateLong = timeParser.parse(endDate).getTime();
		} catch (ParseException e) {  e.printStackTrace(); }

    	// Setting a filter to get only rows from one warehouse
    	Filter ware_filter = new SingleColumnValueFilter(Bytes.toBytes("W"), Bytes.toBytes("ID"), 
    			CompareFilter.CompareOp.EQUAL, Bytes.toBytes(warehouseId));
    	
    	// Setting a filter to get only rows from one district
    	Filter distr_filter = new SingleColumnValueFilter(Bytes.toBytes("D"), Bytes.toBytes("ID"), 
    			CompareFilter.CompareOp.EQUAL, Bytes.toBytes(warehouseId));
    	
    	// Setting filters to get only rows between the two dates
    	Filter start_filter = new SingleColumnValueFilter(Bytes.toBytes("O"), Bytes.toBytes("ENTRY_D"), 
    			CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDateLong));
    	
    	Filter end_filter = new SingleColumnValueFilter(Bytes.toBytes("O"), Bytes.toBytes("ENTRY_D"), 
    			CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endDateLong));
    	
    	// Placing all filters together
    	// By default it uses FilterList.Operator.MUST_PASS_ALL which means an AND between the different filters to use OR use FilterList.Operator.MUST_PASS_ALL
    	FilterList filterList = new FilterList();
    	filterList.addFilter(ware_filter);
    	filterList.addFilter(distr_filter);
    	filterList.addFilter(start_filter);
    	filterList.addFilter(end_filter);

    	// Creation of object scan to retrieve cells from HBase
    	Scan s = new Scan();
    	s.setFilter(filterList);
    	
    	// Retrieve the column C:ID from ORDER table
    	ResultScanner scanner = table.getScanner(s);
    	
    	// Iterate the results to store in a list the customer IDs that satisfy the conditions
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) 
        	customers.add(rr.getValue(Bytes.toBytes("C"), Bytes.toBytes("ID")).toString());

        table.close();
    	
        return customers;

    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
    	
    	String localCustomerId = warehouseId + districtId + customerId;    	
    	
    	HTable query2_table = new HTable(config, "customer");
    	Put put = new Put(Bytes.toBytes(localCustomerId));
    	
    	// In case more than 6 discounts want to be added to customer we only take into account the last 6
    	for(int i = discounts.length > 6 ? discounts.length - 7 : 0; i<discounts.length; i++) {
    		put.add(Bytes.toBytes("C"), Bytes.toBytes("DISCOUNT"), Bytes.toBytes(discounts[i]));
    		query2_table.put(put);
    	}
    	
    	query2_table.close();

    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
    	String localCustomerId = warehouseId + districtId + customerId;    	
    	
    	HTable query3_table = new HTable(config, "customer");
    	Get get = new Get(Bytes.toBytes(localCustomerId));
    	get.setMaxVersions(4);
    	
    	String[] discountArray = new String[4];
    	
    	Result discount = query3_table.get(get); 
    	
    	// Check if row and cell exists in order to avoid null.exception
    	if(query3_table.exists(get) && !discount.isEmpty()) {
    		List<Cell> values = discount.getColumnCells(Bytes.toBytes("C"), Bytes.toBytes("DISCOUNT"));
    	   	
    	    for (int i = 0; i < values.size(); i++) 
    	        discountArray[i] = Bytes.toString( CellUtil.cloneValue( values.get(i) ) );
    	    
    	}
    	else
    		System.out.println("No such client with warehouseId, districtId and customerId \'" + localCustomerId + "\' exists. Empty? "+ query3_table.get(get).isEmpty());
    	
    	query3_table.close();   	
    	return discountArray;
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
        else if(args[1].toUpperCase().equals("DROPTABLES")){
           hBaseTPCC.dropTPCCTables();   
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
