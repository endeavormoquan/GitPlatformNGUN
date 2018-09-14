package cn.colony.lab.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class MyKafkaProducerForShow {
	
	
	private static String messageForType1(){
		String messageStr = null;
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype><typeid>";//typeid
        String str2 = "</typeid></tabletype><inf><satid>";//satid
        String str3 = "</satid><satname>";//satname
        String str4 = "</satname><mass>";//mass
        String str5 = "</mass><aoc>";//aoc
        String str6 = "</aoc></inf></xmlroot>";
        String typeid = "1";
        String satid = "234";
        String satname = "DF4005";
        String mass = "999";
        String aoc = "0.5";
        messageStr = str1+typeid+str2+satid+str3+satname+str4+mass+str5+aoc+str6;
        return messageStr;
	}
	
	private static String messageForType2(){
		String messageStr = null;
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype><typeid>";//typeid
        String str2 = "</typeid></tabletype><inf><devname>";//devname
        String str3 = "</devname><lat>";//lat纬度
        String str4 = "</lat><long>";//long经度
        String str5 = "</long><altitude>";//altitude高度
        String str6 = "</altitude></inf></xmlroot>";
        String typeid = "2";
        String devname = "WeiNanUSB";
        String lat = "111";
        String long_ = "222";
        String altitude = "333";
        messageStr = str1+typeid+str2+devname+str3+lat+str4+long_+str5+altitude+str6;
        return messageStr;
	}
	
	private static String messageForType3(){
		String messageStr = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype><typeid>";//typeid
        String str2 = "</typeid></tabletype><satname><name>";//satname
        String str2_1 = "</name></satname><Command><command>";//command
        String str3 = "</command><commandtime>";//commandtime
        String str4 = "</commandtime></Command>";
        String str5 = "<track><TTCName>";//TTCName
        String str6 = "</TTCName><R>";//R
        String str7 = "</R><A>";//A
        String str8 = "</A></track></xmlroot>";
        String typeid = "3";
        String satname = "TW05A";
        String command = "sayhello";
        String commandtime = "180914";
        String TTCName = "TTCName1";
        String R = "123";
        String A = "234";
        messageStr = str1+typeid+str2+satname+str2_1+command+str3+commandtime+str4+str5+TTCName+str6+R+str7+A+str8;
        return messageStr;
	}
	
	private static String messageForType3_2(){
		String messageStr = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><xmlroot><tabletype><typeid>";//typeid
        String str2 = "</typeid></tabletype><satname><name>";//satname
        String str2_1 = "</name></satname><Command><command>";//command
        String str3 = "</command><commandtime>";//commandtime
        String str4 = "</commandtime></Command>";
        String str5 = "<obc><Tempture>";//Tempture
        String str6 = "</Tempture><states>";//states
        String str7 = "</states></obc></xmlroot>";
        String typeid = "3";
        String satname = "TW05A";
        String command = "sayhelloagain";
        String commandtime = "180915";
        String Tempture = "39";
        String states = "OK";
        messageStr = str1+typeid+str2+satname+str2_1+command+str3+commandtime+str4+str5+Tempture+str6+states+str7;
        return messageStr;
	}
	
	public static void main(String[] args) throws InterruptedException{
		Properties props = new Properties();
		props.put("bootstrap.servers", "master:9092");
		props.put("acks", "all");
		props.put("retries", 1);
		props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        String messageStr = null;
        messageStr = messageForType3_2();
        
        producer.send(new ProducerRecord<String, String>("platformForShow", "Message", messageStr));
        //TODO here to send the testing message
//        for (int i = 0;i<50;i++){
//        	Thread.sleep(50);
//        	System.out.println(i);
//        	String name = names[i%3];
//        	time = new StringBuffer(df.format(new Date())).toString();
//        	String data = UUID.randomUUID().toString();
//        	messageStr = str1+name+str2+time+str3+data+str4;
//        	producer.send(new ProducerRecord<String, String>("platformForShow", "Message", messageStr));
//        }
        producer.close();
	}
}
