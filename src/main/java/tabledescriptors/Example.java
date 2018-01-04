package tabledescriptors;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Example {

	public static void main(String[] args) throws Exception {
	    String target = "Thu Sep 28 20:29:30 JST 2000";
	    DateFormat df = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SSS");
	    Date result1 =  df.parse(args[0]);
	    Date result2 =  df.parse(args[1]);
	    System.out.println(result1.getTime() + " " + result2.getTime());
	}
}