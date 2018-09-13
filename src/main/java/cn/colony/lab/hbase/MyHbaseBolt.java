package cn.colony.lab.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class MyHbaseBolt extends BaseBasicBolt{

	private static final Log LOG = LogFactory.getLog(MyHbaseBolt.class);
	private static final long serialVersionUID = 8098376166042660822L;
	private Configuration hbaseConf = null;
	private Connection connection = null;
	private Admin admin = null;
	private SimpleDateFormat df = null;
	private String timeReversed = null;
	private String rowKey = null;
	private String[] families = {"baseinfo","datainfo"};//暂定hbase数据表有两个列族：基本信息和数据信息
	
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", "master");
			hbaseConf.set("hbase.rootdir", "hdfs://master:9000/hbase");
			hbaseConf.set("zookeeper.znode.parent", "/hbase");
			hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
			connection = ConnectionFactory.createConnection(hbaseConf);
			admin = connection.getAdmin();//TODO do not forget to close the admin? where to close the admin?
			LOG.info("admin got-------------------------------");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.info("admin got failed");
		}
		df = new SimpleDateFormat("yyyyMMddHHmmss");
		super.prepare(stormConf, context);
	}

	private void HBaseOperation(HashMap<String, String> hashMap) throws IOException{
		
		//small table operation
		TableName tableName = TableName.valueOf(hashMap.get("name").getBytes());
		boolean isTableExists = admin.tableExists(tableName);
		
		if (isTableExists == false){
			LOG.info("TABLE:"+hashMap.get("name")+" DOES NOT EXIST, CREATE NOW");
			//为一个新的卫星建立一个新的表
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			for (String family : families){
				HColumnDescriptor cd = new HColumnDescriptor(family);
				descriptor.addFamily(cd);
			}
			admin.createTable(descriptor);
		}
		
		//如果该卫星之前已经创建了表，那么就直接插入数据，到这一步时，一定有一个和该卫星对应的表
		//采用翻转时间戳加上卫星名称的方式设计行键
		timeReversed = new StringBuffer(df.format(new Date())).reverse().toString();
		rowKey = timeReversed + hashMap.get("name");
		Table table = connection.getTable(tableName);
		Put p = new Put(Bytes.toBytes(rowKey));
		//TODO 插入数据：测试时只看固定的几个tag，实际使用时，这里还要琢磨一下
		p.addColumn(Bytes.toBytes(families[0]), Bytes.toBytes("name"), Bytes.toBytes(hashMap.get("name")));
		p.addColumn(Bytes.toBytes(families[0]), Bytes.toBytes("time"), Bytes.toBytes(hashMap.get("time")));
		p.addColumn(Bytes.toBytes(families[1]), Bytes.toBytes("data"), Bytes.toBytes(hashMap.get("data")));
		table.put(p);
		table.close();	
	}
	
	@Test
	public void jsonTest(){
		String jsonString = "{\"satainfo\":{\"keys\":\"name:time:data\",\"time\":\"20180806095544\",\"data\":\"57f77471-e444-4baf-a489-dfa365cdb77d\",\"name\":\"sata1\"}}";
		System.out.println(jsonString);
		JSONObject jsonObject = JSON.parseObject(jsonString);
		String container = jsonObject.getJSONObject("satainfo").getString("keys");
		System.out.println(container);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String jsonString = input.getString(0);
		LOG.info("RECIEVED JSONSTING: " + jsonString);
		JSONObject jsonObject = JSON.parseObject(jsonString);
		
		//TODO consider if the parse is failed
		String container = jsonObject.getJSONObject("satainfo").getString("keys");
		LOG.info("CONTAINER: " + container);
		if (container != null){
			String[] keys = container.split(":");
			
			HashMap<String, String> hashMap = new HashMap<>();
			for (String key : keys){
				String value = jsonObject.getJSONObject("satainfo").getString(key);
				hashMap.put(key, value);
			}
			
			try {
				HBaseOperation(hashMap);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else{
			LOG.error("problems occured when parse, CONTAINER is NULL!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}
