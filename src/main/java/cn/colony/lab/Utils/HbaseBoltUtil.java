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
 * 单例类，提供getAdmin(), getFamilies(), getSimpleDateFormat()，同时提供将jsonString解析成hashmap的工具函数
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
	 * 获得单例连接
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
	 * 获得单例管理员身份
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
	 * 单例列族，弃用
	 * @return String[]
	 */
	public static String[] getFamilies(){
		if (families == null){
			families = new String[]{"satinfo", "measurementcontrol", "outmeasurement", "telemetering", "loadinfo"};
		}
		return families;
	}
	
	/**
	 * 单例SimpleDateFormat
	 * @return java.text.SimpleDateFormat
	 */
	public static SimpleDateFormat getSimpleDateFormat(){
		if (df == null){
			df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		}
		return df;
	}
	
	/**
	 * 将jsonString转换成hashmap，假设jsonString只有两层
	 * @param jsonString
	 * @return a hashmap that contains two steps.
	 */
	public static HashMap<String, HashMap<String, String>> parseJsonStringToHashmap(String jsonString){
		JSONObject jsonObject = JSONObject.parseObject(jsonString);
		HashMap<String, HashMap<String, String>> rootmap = new HashMap<>();
		JSONObject containerFirst = jsonObject.getJSONObject("xmlroot");
		Set<String> jsonSetFirst = containerFirst.keySet();//获取第一层keys，其实就是列族名
		for (String str : jsonSetFirst){//通过第一层keys遍历第一层
			JSONObject containerSecond = containerFirst.getJSONObject(str);//获取第二层json object
			Set<String> jsonSetSecond = containerSecond.keySet();//获取第二层keys，列限定符
			HashMap<String, String> branchmap = new HashMap<>();
			for (String key : jsonSetSecond){//遍历第二层
				String value = containerSecond.getString(key);//列限定符中的数据
//				LOG.info(key + ":" + value);
				branchmap.put(key, value);
			}
			rootmap.put(str, branchmap);
		}
		return rootmap;
	}

}
