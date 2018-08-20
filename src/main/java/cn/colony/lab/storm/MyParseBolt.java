package cn.colony.lab.storm;

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


public class MyParseBolt extends BaseBasicBolt{

	private static final Log LOG = LogFactory.getLog(MyParseBolt.class);
	private static final long serialVersionUID = 2398643366107201062L;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String xml = input.getString(0);
		LOG.info("RECIEVED XML: "+xml);
		JSONObject jsonObject = null;
		try {
			jsonObject = XmlToJsonUtil.xmltoJsonObject(xml);
			String jsonString = jsonObject.toString();
			LOG.info("PARSE RESULT: "+jsonString);
			collector.emit(new Values(jsonString));
		} catch (Exception e) {
			LOG.info("PARSE FAILED");
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("JsonString"));
	}

}
