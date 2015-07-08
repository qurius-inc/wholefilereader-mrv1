package mr.wholeFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;

public class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {

	private FileSplit split;
	private Configuration conf;
	
	private boolean fileProcessed = false;

	public WholeFileRecordReader(Configuration job, InputSplit split) throws IOException {
		this.split = (FileSplit) split;
		this.conf = job;
	}
	
	@Override
	public boolean next(NullWritable key, BytesWritable value) throws IOException {
		if ( fileProcessed ){
			return false;
		}
		
		int fileLength = (int)split.getLength();
		byte [] result = new byte[fileLength];
		
		FileSystem  fs = FileSystem.get(conf);
		FSDataInputStream in = null; 
		try {
			in = fs.open( split.getPath());
			IOUtils.readFully(in, result, 0, fileLength);
			value.set(result, 0, fileLength);
			
		} finally {
			IOUtils.closeStream(in);
		}
		this.fileProcessed = true;
		return true;
	}

    @Override
    public long getPos() throws IOException {
        return 0L;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// nothing to close
	}

}
