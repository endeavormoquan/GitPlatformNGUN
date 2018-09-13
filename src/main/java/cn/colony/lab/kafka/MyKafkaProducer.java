package cn.colony.lab.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class MyKafkaProducer {
	
	@Test
	public void messageTest(){
		String messageStr = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = null;
        String[] names = {"sata1","sata2","sata3"};
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><satainfo><name>";
        String str2 = "</name><time>";
        String str3 = "</time><data>";
        String str4 = "</data><keys>name:time:data</keys></satainfo>";
        //TODO here to sent the testing message!
        for (int i = 0;i<1;i++){
        	String name = names[i%3];
        	time = new StringBuffer(df.format(new Date())).toString();
        	String data = UUID.randomUUID().toString();
        	messageStr = str1+name+str2+time+str3+data+str4;
        	System.out.println(messageStr);
        }
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
        String str1 = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><satainfo><name>";
        String str2 = "</name><time>";
        String str3 = "</time><data>";
        String str4 = "</data><keys>name:time:data</keys></satainfo>";
        //TODO here to send the testing message
        for (int i = 0;i<50;i++){
        	Thread.sleep(50);
        	System.out.println(i);
        	String name = names[i%3];
        	time = new StringBuffer(df.format(new Date())).toString();
        	String data = UUID.randomUUID().toString();
        	messageStr = str1+name+str2+time+str3+data+str4;
        	producer.send(new ProducerRecord<String, String>("platform1", "Message", messageStr));
        }
        producer.close();
	}
}
