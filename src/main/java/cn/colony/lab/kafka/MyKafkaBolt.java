package cn.colony.lab.kafka;

import java.util.Arrays;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class MyKafkaBolt {
	
	private String zks = null;
	private String topic = null;
	private String zkRoot = null;
	private String id = null;
	private String brokerPath = null;
	private String[] zkServers = null;
	private KafkaSpout kafkaSpout = null;
    
    public MyKafkaBolt(String zks, String topic, String zkRoot, String id, String brokerPath, String[] zkServers){
    	this.setZks(zks);
    	this.setTopic(topic);
    	this.setZkRoot(zkRoot);
    	this.setId(id);
    	this.setBrokerPath(brokerPath);
    	this.setZkServers(zkServers);
    	
    	BrokerHosts brokerHosts = new ZkHosts(zks,brokerPath);
    	SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
    	spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    	spoutConf.zkServers = Arrays.asList(zkServers);
    	spoutConf.zkPort = 2181;
    	spoutConf.ignoreZkOffsets = false;
    	
    	kafkaSpout = new KafkaSpout(spoutConf);
    }

	/**
	 * @return the zks
	 */
	public String getZks() {
		return zks;
	}

	/**
	 * @param zks the zks to set
	 */
	private void setZks(String zks) {
		this.zks = zks;
	}

	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @param topic the topic to set
	 */
	private void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * @return the zkRoot
	 */
	public String getZkRoot() {
		return zkRoot;
	}

	/**
	 * @param zkRoot the zkRoot to set
	 */
	private void setZkRoot(String zkRoot) {
		this.zkRoot = zkRoot;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	private void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the brokerPath
	 */
	public String getBrokerPath() {
		return brokerPath;
	}

	/**
	 * @param brokerPath the brokerPath to set
	 */
	private void setBrokerPath(String brokerPath) {
		this.brokerPath = brokerPath;
	}

	/**
	 * @return the zkServers
	 */
	public String[] getZkServers() {
		return zkServers;
	}

	/**
	 * @param zkServers the zkServers to set
	 */
	private void setZkServers(String[] zkServers) {
		this.zkServers = zkServers;
	}

	/**
	 * @return the kafkaSpout according to the para you give
	 */
	public KafkaSpout getKafkaSpout() {
		return kafkaSpout;
	}

}
