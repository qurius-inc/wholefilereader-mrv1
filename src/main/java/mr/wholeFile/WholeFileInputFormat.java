package mr.wholeFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> getRecordReader(
			InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {
		WholeFileRecordReader reader = new WholeFileRecordReader(job, inputSplit);
		return reader;
	}


}
