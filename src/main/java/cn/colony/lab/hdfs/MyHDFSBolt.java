package cn.colony.lab.hdfs;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

public class MyHDFSBolt {

	private static HdfsBolt hdfsBolt = null;
	
	public MyHDFSBolt() {
		RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		FileRotationPolicy rotationPolicy = new TimedRotationPolicy(2.0f, TimeUnit.MINUTES );
		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/sata/").withPrefix("").withExtension(".data");
		this.hdfsBolt = new HdfsBolt()
				.withFsUrl("hdfs://master:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
	}

	/**
	 * @return the hdfsBolt
	 */
	public HdfsBolt getMyHdfsBolt() {
		return hdfsBolt;
	}
}
