package cn.colony.lab.topology;

import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

import cn.colony.lab.hbase.MyHbaseBolt;
import cn.colony.lab.hbase.MyHbaseHashMapBolt;
import cn.colony.lab.hdfs.MyHDFSBolt;
import cn.colony.lab.kafka.MyKafkaBolt;
import cn.colony.lab.storm.MyParseBolt;

public class MyTopologyForShow {

	private static final Log LOG = LogFactory.getLog(MyTopologyForTest.class);
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException{
		
		//create a kafka spout
		String zks = "master:2181";
		String topic = "platformForShow";
		String zkRoot = "/storm";
		String id = UUID.randomUUID().toString();
		String brokerPath = "/kafka/brokers";
		String[] zkServers = {"master"};
		
		KafkaSpout kafkaSpout = new MyKafkaBolt(zks, topic, zkRoot, id, brokerPath, zkServers).getKafkaSpout();
		
		//简单地将收到的解析过的xml文件存到hdfs中
		HdfsBolt hdfsBolt = new MyHDFSBolt().getMyHdfsBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", kafkaSpout, 1);
		builder.setBolt("parse-bolt", new MyParseBolt(), 1).shuffleGrouping("kafka-reader");
//		builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("parse-bolt");
		builder.setBolt("hbase-hashmap-bolt", new MyHbaseHashMapBolt(), 1).shuffleGrouping("parse-bolt");
		
		Config conf = new Config();
		conf.put(Config.NIMBUS_HOST, "master");
		conf.setNumWorkers(4);
		StormSubmitter.submitTopology(MyTopologyForShow.class.getSimpleName(), conf, builder.createTopology());
	}
}
