package cn.colony.lab.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;

import cn.colony.lab.Utils.HbaseBoltUtil;
import cn.colony.lab.Utils.XmlToJsonUtil;


public class MyHbaseHashMapBoltForShow extends BaseBasicBolt{

	private static final Log LOG = LogFactory.getLog(MyHbaseHashMapBolt.class);
	private static final long serialVersionUID = 3833590169869172234L;
	private Admin admin = null;
	private Connection connection = null;
	private SimpleDateFormat df = null;
	private String[] families = null;
	private String dateReversed = null;
	private String rowKey = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	try {
			connection = HbaseBoltUtil.getConnection();
			LOG.info("CONNECTION GOT");
			admin = HbaseBoltUtil.getAdmin();
			LOG.info("ADMIN GOT");
			df = HbaseBoltUtil.getSimpleDateFormat();
			families = HbaseBoltUtil.getFamilies();
		} catch (Exception e) {
			LOG.error("HBASE CONNECTION OR ADMIN GET FAILED");
			e.printStackTrace();
		}
		super.prepare(stormConf, context);
	}
	
	/**
	 * create table that named with tableName, and the definition of families of the table comes from HbaseBoltUtil.class
	 * @param tableName name of the table
	 * @throws Exception
	 */
	private void createTable(TableName tableName) throws Exception{
		//TODO create table here, small and big table.
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		for (String family : families){
			HColumnDescriptor cd = new HColumnDescriptor(family);
			descriptor.addFamily(cd);
		}
		admin.createTable(descriptor);
	}
	
	/**
	 * create tables for each sat, each sat differs from others with its id. data will be inserted into the table according to the hashmap
	 * @param rootmap hashmap parsed from xml
	 * @throws Exception
	 */
	private void operateWithSmallTable(HashMap<String, HashMap<String, String>> rootmap) throws Exception{	
		//small table operation
		//采用satid作为表名
		TableName tableName = TableName.valueOf(rootmap.get("satinfo").get("satid").getBytes());
		boolean isTableExists = admin.tableExists(tableName);
		
		if (isTableExists == false){
			LOG.info("SMALL TABLE:" + tableName.getNameAsString() + "DOES NOT EXIST, CREATE NOW");
			try{
				createTable(tableName);
			}catch (Exception e){			
				e.printStackTrace();
				LOG.fatal("TABLE CREATED FAILED");
			}
		}
		
		if (admin.tableExists(tableName) == true){
			//每个卫星单独的表采用时间翻转作为行键
			dateReversed = new StringBuffer(df.format(new Date())).reverse().toString();
			rowKey = dateReversed;
			Table table = connection.getTable(tableName);
			Put p = getPut(rootmap, rowKey);
			table.put(p);
			table.close();
		}
		else{
			LOG.warn("DATA DISCARED CAUSE NO SMALL TABLE CAN BE CONNECTED");
		}
	}
	
	/**
	 * create a table witch can hold all the data from all the sat. the table name is 'SATALL'.
	 * the rowkey of the table is the reversion of time and satid.
	 * @param rootmap hashmap parsed from xml
	 * @throws Exception
	 */
	private void operateWithBigTable(HashMap<String, HashMap<String, String>> rootmap) throws Exception {
		// TODO operateWithBigTable	
		//总表的表名为	SATALL
		TableName tableName = TableName.valueOf(Bytes.toBytes("SATALL"));
		boolean isTableExists = admin.tableExists(tableName);
		if (isTableExists == false){
			LOG.info("BIG TABLE:" + tableName.getNameAsString() + "DOES NOT EXIST, CREATE NOW");
			try{
				createTable(tableName);
			}catch (Exception e){			
				e.printStackTrace();
				LOG.fatal("TABLE CREATED FAILED");
			}
		}
		
		if (admin.tableExists(tableName) == true){
			//大表采用翻转时间+卫星id作为行键
			dateReversed = new StringBuffer(df.format(new Date())).reverse().toString();
			rowKey = dateReversed + rootmap.get("satinfo").get("satid");
			Table table = connection.getTable(tableName);
			Put p = getPut(rootmap, rowKey);
			table.put(p);
			table.close();
		}
		else{
			LOG.warn("DATA DISCARED CAUSE NO BIG TABLE CAN BE CONNECTED");
		}
	}
	
	/**
	 * get Put from hashmap and rowkey, which will insert to the table.
	 * @param map hashmap that parsed from xml
	 * @param rowKey rowkey of the hbase table
	 * @return hbase.client.Put
	 */
	private Put getPut(HashMap<String, HashMap<String, String>> map, String rowKey){
		Put p = new Put(Bytes.toBytes(rowKey));
		//遍历hashmap插入数据
		for (Entry<String, HashMap<String, String>> entry1 : map.entrySet()){
			String familyName = entry1.getKey();
			HashMap<String, String> value1 = entry1.getValue();
			for (Entry<String, String> entry2 : value1.entrySet()){
				String qualifier = entry2.getKey();
				String data = entry2.getValue();
				p.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
			}
		}
		return p;
	}
	
	
	
	private void operateWithId1(HashMap<String, HashMap<String, String>> rootmap){
		int id = 1;
		TableName tablename = TableName.valueOf(Bytes.toBytes("SatInf"));
		boolean isTableExists = admin.tableExists(tablename);
		
		if (isTableExists == false){
			LOG.info("table:"+tablename+" does not exist,create now");
			try{
				createTable(tableName);
			}catch(Exception e){
				e.printStackTrace();
				LOG.fatal("table created failed");
			}
		}
		
		if (admin.tableExists(tablename) == true){
			rowKey = rootmap.get("inf").get("satname");
			Table table = connection.getTable(tablename);
			
			Put p = new Put(Bytes.toBytes(rowKey));
			for (Entry<String,HashMap<String, String>> entry1 : rootmap.entrySet()){
				String familyName = entry1.getKey();
				if (familyName.equals("tabletype")) continue;
				
			}
		}
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String input = tuple.getString(0);//收到的是json string
		LOG.info("RECEIVED JSONSTRING: " + input);
		
		//将得到的json string解析成有一层嵌套的hashmap
		HashMap<String, HashMap<String, String>> rootmap = HbaseBoltUtil.parseJsonStringToHashmap(input);
		
		/**
		 * 简单的数据完备性校验，每一次传来的数据都必须要有表类别信息，否则该条数据作废。表类别用来说明当前数据是何种数据
		 * id = 1:SatInf	记录每个卫星的基本信息
		 * id = 2:TTCInf	记录每个测控站的基本信息
		 * id = 3:SatName	卫星实时数据
		 */
		//TODO not finised
		String typeid = rootmap.get("tabletype").get("typeid");
		if (typeid != null){
			try{
				if (typeid.equals("1"))
					operateWithId1(rootmap);
			}catch (Exception e){
				e.printStackTrace();
			}
			try{
				if (typeid.equals("2"))
					operateWithId2(rootmap);
			}catch (Exception e){
				e.printStackTrace();
			}
			try{
				if (typeid.equals("3"))
					operateWithId3(rootmap);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		else{
			LOG.warn("wrong jsonString or rootmap, no typeid found!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Test
	public void testParseJsonStringToHashmap(){
		String xml = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype>"+
						"<typeid>1</typeid>"+
						"</tabletype>"+
						"<family><qualifier>data</qualifier></family></xmlroot>";
		JSONObject jsonObject = null;
		String jsonString = null;
		try {
			jsonObject = XmlToJsonUtil.xmltoJsonObject(xml);
			jsonString = jsonObject.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (jsonString != null){
			HashMap<String, HashMap<String, String>> rootmap = HbaseBoltUtil.parseJsonStringToHashmap(jsonString);
			//遍历hashmap
			for (Entry<String, HashMap<String, String>> entry1 : rootmap.entrySet()){
				String familyName = entry1.getKey();
				HashMap<String, String> value1 = entry1.getValue();
				for (Entry<String, String> entry2 : value1.entrySet()){
					String qualifier = entry2.getKey();
					String data = entry2.getValue();
					System.out.println(familyName+":"+qualifier+":"+data);
				}
			}
		}
	}
}
