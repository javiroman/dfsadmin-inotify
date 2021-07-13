package org.inotify;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

public class HdfsINotify {

	public static void main(String[] args) throws
			IOException, InterruptedException, MissingEventsException {

		long lastReadTxid = 0;
		DFSInotifyEventInputStream eventStream;
		EventManagement em = new EventManagement();

		ShutdownHook sample = new ShutdownHook();
		sample.attachShutDownHook();

		if (args.length > 1) {
			lastReadTxid = Long.parseLong(args[1]);
		}

		HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), new Configuration());

		if (lastReadTxid == 0) {
			eventStream = admin.getInotifyEventStream();
		} else {
			eventStream = admin.getInotifyEventStream(lastReadTxid);
		}

		boolean rawMode = false;
		if (System.getProperty("Raw") != null) {
			rawMode = System.getProperty("Raw", "false").equals("true");
		}

		while (true) {
			EventBatch batch = eventStream.take();
			for (Event event : batch.getEvents()) {
				if (rawMode) {
					System.out.println(event);
				} else {
					String outputString = new
							String(em.getJsonFormat(event, event.getEventType(), batch.getTxid(), args[2]));
					if (outputString != "{}") {
						System.out.println(outputString);
					}
				}
			}
		}
	}

	/*
	  Inner class controlling console control-c.
	 */
	public static class ShutdownHook {
		public void attachShutDownHook() {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					System.out.println("Exit by user.");
				}
			});

		}
	}
}