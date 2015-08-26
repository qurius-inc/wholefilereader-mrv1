package mr.wholeFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONValue;

public class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {

	private FileSplit split;
	private Configuration conf;
	
	private boolean fileProcessed = false;
    private final String hiveDelimiter = "\001";

	public WholeFileRecordReader(Configuration job, InputSplit split) throws IOException {
		this.split = (FileSplit) split;
		this.conf = job;
	}
	
	@Override
	public boolean next(NullWritable key, BytesWritable value) throws IOException {
		if ( fileProcessed ){ return false; }

//        int fileLength = (int) split.getLength();
//		byte[] fileArray = new byte[fileLength];

		FileSystem  fs = FileSystem.get(conf);
		FSDataInputStream in = null; 
		try {
			in = fs.open(split.getPath());

            Map<String, String> m = (Map) JSONValue.parse(IOUtils.toString(in, StandardCharsets.UTF_8));
            String b = new StringBuilder()
                    .append(m.get("title")).append(hiveDelimiter)
                    .append(m.get("author")).append(hiveDelimiter)
                    .append(m.get("date")).append(hiveDelimiter)
                    .append(m.get("content")).toString();
            byte[] results = b.getBytes(StandardCharsets.UTF_8);

			value.set(results, 0, results.length);
		} finally {
//			IOUtils.closeStream(in);
            in.close();
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
