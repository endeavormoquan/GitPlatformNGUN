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
	
	@Test
	public void messageTest(){
		String messageStr = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = null;
        String[] names = {"sata1","sata2","sata3"};
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
        messageStr = str1+typeid+str2+satid+str3+satname+str4+mass+str5+aoc+str6;
        System.out.println(messageStr);
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
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = null;
        String[] names = {"sata1","sata2","sata3"};
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
        messageStr = str1+typeid+str2+satid+str3+satname+str4+mass+str5+aoc+str6;
//        producer.send(new ProducerRecord<String, String>("platformForShow", "Message", messageStr));
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
