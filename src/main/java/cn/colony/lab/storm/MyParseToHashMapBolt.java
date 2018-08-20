package cn.colony.lab.storm;

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSONObject;

import cn.colony.lab.Utils.XmlToJsonUtil;


/**
 * deprecated, cause HashMap cannot be emitted.?
 * @author ColonyAlbert
 *
 */
public class MyParseToHashMapBolt extends BaseBasicBolt{

	private static final Log LOG = LogFactory.getLog(MyParseToHashMapBolt.class);
	private static final long serialVersionUID = -7639529209992476662L;

	/**
	 * 
	 * @param String xml，根节点必须是"xmlroot"
	 * @return 两层嵌套的哈希表，第一层是大类，第二层是具体数据
	 * @throws Exception
	 */
	private HashMap<String, HashMap<String, String>> parseXmlToHashmap(String xml) throws Exception{
		JSONObject jsonObject = XmlToJsonUtil.xmltoJsonObject(xml);
		String jsonString = jsonObject.toString();
		System.out.println(jsonString);
		HashMap<String, HashMap<String, String>> rootmap = new HashMap<>();
		JSONObject containerFirst = jsonObject.getJSONObject("xmlroot");
		Set<String> jsonSetFirst = containerFirst.keySet();//获取第一层keys
		for (String str : jsonSetFirst){//通过第一层keys遍历第一层
			System.out.println(str);			
			JSONObject containerSecond = containerFirst.getJSONObject(str);//获取第二层json object
			Set<String> jsonSetSecond = containerSecond.keySet();//获取第二层keys
			
			HashMap<String, String> branchmap = new HashMap<>();
			for (String key : jsonSetSecond){//遍历第二层
				System.out.println(key);
				String value = containerSecond.getString(key);
				branchmap.put(key, value);
			}
			rootmap.put(str, branchmap);
		}
		return rootmap;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String xml = input.getString(0);
		LOG.info("RECIEVED XML: "+xml);
		try {
			HashMap<String, HashMap<String, String>> rootmap = this.parseXmlToHashmap(xml);
			LOG.info("PARSE SUCCESSED, EMIT ROOTMAP");
			collector.emit(new Values(rootmap));
		} catch (Exception e1) {
			LOG.info("PARSE FAILED");
			e1.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashmap"));
	}
}
