package it.cnr.isti.hpclab.finegrained;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public final class TinyJProfiler {
	private static TinyJProfiler INSTANCE;
	private static String OUTPUT_FILE = "/data/TinyJProfiler.json";
	
	ArrayBlockingQueue<String> buffer;
	
	Thread writer;
	private volatile boolean running = true;

	private TinyJProfiler(){
		/** Create a buffer for logs */
		buffer = new ArrayBlockingQueue<String>(1 << 10);
		
		/** Initialize a thread for asynchronous writing to file */
		writer = new FileWritingThread();
		writer.start();
	}
	
	public static void close(){
		getInstance().running = false;
	}

	public static TinyJProfiler getInstance(){
		if(INSTANCE == null){
			INSTANCE = new TinyJProfiler();
		}
		return INSTANCE;
	}
	
	public static void tic() {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String method = stackTraceElements[2].getMethodName();
		long time = System.nanoTime();
		long threadId = Thread.currentThread().getId();
		String className = stackTraceElements[2].getClassName();
		
		try {
			getInstance().buffer.put("{\"thread\":"+threadId+",\"className\":\""+className+"\",\"method\":\""+method+"\",\"type\":\"tic\",\"time\":"+time+"}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void tic(String name) {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String method = name;
		long time = System.nanoTime();
		long threadId = Thread.currentThread().getId();
		String className = stackTraceElements[2].getClassName();
		
		try {
			getInstance().buffer.put("{\"thread\":"+threadId+",\"className\":\""+className+"\",\"method\":\""+method+"\",\"type\":\"tic\",\"time\":"+time+"}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void toc() {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String method = stackTraceElements[2].getMethodName();
		long time = System.nanoTime();
		long threadId = Thread.currentThread().getId();
		String className = stackTraceElements[2].getClassName();
		
		try {
			getInstance().buffer.put("{\"thread\":"+threadId+",\"className\":\""+className+"\",\"method\":\""+method+"\",\"type\":\"toc\",\"time\":"+time+"}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void toc(String name) {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String method = name;
		long time = System.nanoTime();
		long threadId = Thread.currentThread().getId();
		String className = stackTraceElements[2].getClassName();
		
		try {
			getInstance().buffer.put("{\"thread\":"+threadId+",\"className\":\""+className+"\",\"method\":\""+method+"\",\"type\":\"toc\",\"time\":"+time+"}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private class FileWritingThread extends Thread {
		private FileWriter fw;
		private BufferedWriter bw;
		
		public FileWritingThread() {
			/** Open the file for storing output data */
			try {
				File file = new File(OUTPUT_FILE);
				fw = new FileWriter(file);
				bw = new BufferedWriter(fw);
			} catch (IOException e){
				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			while (running) {
				try {
					bw.write(buffer.take());
					bw.newLine();
				} catch (InterruptedException e){
					e.printStackTrace();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
			
			/** Flush the last data and close the file */
			while (buffer.size() > 0) {
				try {
					bw.write(buffer.take());
					bw.newLine();
				} catch (InterruptedException e){
					e.printStackTrace();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e){
				e.printStackTrace();
			}
			
			System.out.println("TinyJProfiler: file writer terminated with "+buffer.size()+" pending logs");
		}
	}
}