package cn.colony.lab.hbase;

import java.io.IOException;
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

	private static final Log LOG = LogFactory.getLog(MyHbaseHashMapBoltForShow.class);
	private static final long serialVersionUID = 3833590169869172234L;
	private Admin admin = null;
	private Connection connection = null;
	private SimpleDateFormat df = null;
	private String[] families = null;
	private String rowKey = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	try {
			connection = HbaseBoltUtil.getConnection();
			LOG.info("CONNECTION GOT");
			admin = HbaseBoltUtil.getAdmin();
			LOG.info("ADMIN GOT");
			df = HbaseBoltUtil.getSimpleDateFormat();
		} catch (Exception e) {
			LOG.error("HBASE CONNECTION OR ADMIN GET FAILED");
			e.printStackTrace();
		}
		super.prepare(stormConf, context);
	}
	
	
	/**
	 * 根据hashmap和rowkey创建一个能够插入到表中的Put实例
	 * @param map hashmap that parsed from xml
	 * @param rowKey rowkey of the hbase table
	 * @return hbase.client.Put
	 */
	private Put getPut(HashMap<String, HashMap<String, String>> rootmap, String rowKey){
		Put p = new Put(Bytes.toBytes(rowKey));
		for (Entry<String,HashMap<String, String>> entry1 : rootmap.entrySet()){
			String familyName = entry1.getKey();
			//tabletype是每个xml都有的，satname是只有第三类表才有的，用来指定卫星实时数据是哪个卫星，这里的satname是xml中的第一级,xmlroot:satname:name
			//在第一类表中也有satname，但是它是第二级中的,xmlroot:inf:satname，两个satname不要混淆
			if (familyName.equals("tabletype") || familyName.equals("satname")) continue;
			HashMap<String, String> value1 = entry1.getValue();
			for (Entry<String, String> entry2 : value1.entrySet()){
				String qualifier = entry2.getKey();
				String data = entry2.getValue();
				p.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
			}
		}
		return p;
	}
	
	/**
	 * 根据表名称和类型码创建一个表，每个表有不一样的列族
	 * @param tableName 表名称
	 * @param typeId 表类型码
	 * @throws IOException
	 */
	private void createTable(TableName tableName, String typeId) throws IOException{
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		//对应三个基本表各自不同的列族
		if (typeId.equals("1"))
			families = new String[]{"inf"};
		else if (typeId.equals("2"))
			families = new String[]{"inf"};
		else if (typeId.equals("3"))
			families = new String[]{"Command","track","obc","power","GNC","com"};
		
		for (String family : families){
			HColumnDescriptor cd = new HColumnDescriptor(family);
			descriptor.addFamily(cd);
		}
		admin.createTable(descriptor);
	}
	
	/**
	 * 根据rootmap和typeid创建尚未创建的表，并插入数据
	 * @param rootmap 由xml解析得到的hashmap
	 * @param typeId id = 1:SatInf	记录每个卫星的基本信息
		 			 id = 2:TTCInf	记录每个测控站的基本信息
		 			 id = 3:SatName	卫星实时数据
	 * @throws IOException
	 */
	private void operateWithTable(HashMap<String, HashMap<String, String>> rootmap,String typeId){
		TableName tableName = null;
		if (typeId.equals("1"))
			tableName = TableName.valueOf(Bytes.toBytes("SatInf"));
		else if (typeId.equals("2"))
			tableName = TableName.valueOf(Bytes.toBytes("TTCInf"));
		else if (typeId.equals("3"))
			tableName = TableName.valueOf(Bytes.toBytes(rootmap.get("satname").get("name")));
		
		LOG.info("tablename get from rootmap:"+tableName.toString());
		boolean isTableExists = false;
		try {
			isTableExists = admin.tableExists(tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			LOG.error("admin.tableExists failed");
			e1.printStackTrace();
		}
		
		if (isTableExists == false){
			LOG.info("table:"+tableName+" does not exist,create now");
			try{
				createTable(tableName,typeId);
			}catch(Exception e){
				e.printStackTrace();
				LOG.fatal("table created failed");
			}
		}
		
		try {
			isTableExists = admin.tableExists(tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			LOG.error("admin.tableExists failed");
			e1.printStackTrace();
		}
		
		if (isTableExists == true){
			if (typeId.equals("1"))
				rowKey = rootmap.get("inf").get("satname");				
			else if (typeId.equals("2"))
				rowKey = rootmap.get("inf").get("devname");
			else if (typeId.equals("3"))
				rowKey = new StringBuffer(df.format(new Date())).toString();
			
			Table table;
			try {
				table = connection.getTable(tableName);
				Put p = getPut(rootmap, rowKey);		
				table.put(p);
				table.close();
			} catch (IOException e) {
				LOG.error("connection.getTable failed or table.put or table.close failed");
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String input = tuple.getString(0);//收到的是json string
		LOG.info("received json string: " + input);
		
		//将得到的json string解析成有一层嵌套的hashmap
		HashMap<String, HashMap<String, String>> rootmap = HbaseBoltUtil.parseJsonStringToHashmap(input);
		
		/**
		 * 简单的数据完备性校验，每一次传来的数据都必须要有表类别信息，否则该条数据作废。
		 * id = 1:SatInf	记录每个卫星的基本信息
		 * id = 2:TTCInf	记录每个测控站的基本信息
		 * id = 3:SatName	卫星实时数据
		 */
		String typeId = rootmap.get("tabletype").get("typeid");
		if (typeId != null){
			LOG.info("tabletypeid get");
			try{
				operateWithTable(rootmap,typeId);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		else{
			LOG.error("wrong jsonString or rootmap, no typeid found!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Test
	public void testParseJsonStringToHashmap(){
		String xml = null;
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype><typeid>";//typeid
        String str2 = "</typeid></tabletype><inf><satid>";//satid
        String str3 = "</satid><satname>";//satname
        String str4 = "</satname><mass>";//mass
        String str5 = "</mass><aoc>";//aoc
        String str6 = "</aoc></inf></xmlroot>";
        String typeid = "1";
        String satid = "123";
        String satname = "FY2001";
        String mass = "888";
        String aoc = "0.2";
        xml = str1+typeid+str2+satid+str3+satname+str4+mass+str5+aoc+str6;
		
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
