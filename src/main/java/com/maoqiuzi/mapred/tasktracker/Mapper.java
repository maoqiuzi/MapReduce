package com.maoqiuzi.mapred.tasktracker;

//import common.Hdfs;
//import jobtracker.InputSplit;

import com.maoqiuzi.mapred.jobtracker.InputSplit;
import com.maoqiuzi.mapred.common.Hdfs;

import java.io.*;
import java.util.ArrayList;

/**
 * K1 and V1 here should be Long and String for this very version of mapreduce.
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public abstract class Mapper<K1, V1, K2, V2> {
	public abstract void map(K1 key, V1 value, TaskTrackerContext context);

	public SlaveStatus run(TaskTrackerContext context) {
		InputStream inputStream = readInputSplit(context.getInputSplit());
		System.out.println("inputSplit readed");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			Long offset = 0L;
			String line;
			context.setMapperResult(new ArrayList<ReducerAssignment>());

			while (null != (line = reader.readLine())) {
				map((K1)offset, (V1)line, context);
				offset = offset + line.getBytes().length;
			}
			return SlaveStatus.MAPSUCCEEDED;
		} catch (IOException ioe) {
			System.err.println("Exception occurs when mapping");
			ioe.printStackTrace();
			return SlaveStatus.MAPFAILED;
		}
	}
	private InputStream readInputSplit(InputSplit inputSplit) {
		try {
			Hdfs hdfs = new Hdfs();
			System.out.printf("reading %s from S3", inputSplit.getPath() + "/" + inputSplit.getName());
			return hdfs.readS3(inputSplit.getPath()+"/"+inputSplit.getName(), inputSplit.getStartOffset(), inputSplit.getEndOffset());
		} catch (IOException ioe) {
			System.err.println("Exception occurs when read input split from Amazon S3");
			ioe.printStackTrace();
			return null;
		}
	}
}
