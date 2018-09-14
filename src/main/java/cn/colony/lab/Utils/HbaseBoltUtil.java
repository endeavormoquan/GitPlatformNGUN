package cn.colony.lab.Utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * �����࣬�ṩgetAdmin(), getFamilies(), getSimpleDateFormat()��ͬʱ�ṩ��jsonString������hashmap�Ĺ��ߺ���
 * @author ColonyAlbert
 *
 */
public class HbaseBoltUtil {

	private static final Log LOG = LogFactory.getLog(HbaseBoltUtil.class);
	private static Configuration hbaseConf = null;
	private static Connection connection = null;
	private static Admin admin = null;
	private static SimpleDateFormat df = null;
	private static String[] families = null;
	
	/**
	 * ��õ�������
	 * @return hbase.client.Connection
	 * @throws IOException
	 */
	public static Connection getConnection() throws IOException{
		if (connection == null){
			hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", "master");
			hbaseConf.set("hbase.rootdir", "hdfs://master:9000/hbase");
			hbaseConf.set("zookeeper.znode.parent", "/hbase");
			hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
			connection = ConnectionFactory.createConnection(hbaseConf);
		}
		return connection;
	}
	
	/**
	 * ��õ�������Ա���
	 * @return hbase.client.Admin
	 * @throws Exception
	 */
	public static Admin getAdmin() throws Exception{
		if (admin == null){
			Connection connection = HbaseBoltUtil.getConnection();
			admin = connection.getAdmin();
		}
		return admin;
	}
	
	/**
	 * �������壬����
	 * @return String[]
	 */
	public static String[] getFamilies(){
		if (families == null){
			families = new String[]{"satinfo", "measurementcontrol", "outmeasurement", "telemetering", "loadinfo"};
		}
		return families;
	}
	
	/**
	 * ����SimpleDateFormat
	 * @return java.text.SimpleDateFormat
	 */
	public static SimpleDateFormat getSimpleDateFormat(){
		if (df == null){
			df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		}
		return df;
	}
	
	/**
	 * ��jsonStringת����hashmap������jsonStringֻ������
	 * @param jsonString
	 * @return a hashmap that contains two steps.
	 */
	public static HashMap<String, HashMap<String, String>> parseJsonStringToHashmap(String jsonString){
		JSONObject jsonObject = JSONObject.parseObject(jsonString);
		HashMap<String, HashMap<String, String>> rootmap = new HashMap<>();
		JSONObject containerFirst = jsonObject.getJSONObject("xmlroot");
		Set<String> jsonSetFirst = containerFirst.keySet();//��ȡ��һ��keys����ʵ����������
		for (String str : jsonSetFirst){//ͨ����һ��keys������һ��
			JSONObject containerSecond = containerFirst.getJSONObject(str);//��ȡ�ڶ���json object
			Set<String> jsonSetSecond = containerSecond.keySet();//��ȡ�ڶ���keys�����޶���
			HashMap<String, String> branchmap = new HashMap<>();
			for (String key : jsonSetSecond){//�����ڶ���
				String value = containerSecond.getString(key);//���޶����е�����
//				LOG.info(key + ":" + value);
				branchmap.put(key, value);
			}
			rootmap.put(str, branchmap);
		}
		return rootmap;
	}

}
