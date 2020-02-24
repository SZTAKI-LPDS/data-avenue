package hu.sztaki.lpds.dataavenue.adaptors.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author Eniko Nagy
 *
 * This class implements multipart download of objects in parallel (threaded) way.
 * TODO This function is not yet intergrated into the plugin.
 */
public class ThreadedMultipartDownloadInputStream extends InputStream {

	// constans
	public final static int NUM_THREADS = 10;
	public final static int PART_SIZE = 5 * 1024 * 1024; // 5 MB
	public final static int BUFFER_SIZE = 16 * 1024;
	private List<PartDownloadThread> listOfThreads;
	private ExecutorService threadPool;

	private byte[][] parts = new byte[NUM_THREADS][PART_SIZE]; // reuse part array

	/**
	 * Description: constructor of the MultipartDownloadInputStream class This class
	 * will download from an S3 storage a file in parallel
	 * 
	 * @param amazonS3Client:
	 *            the owner of the target S3 storage
	 * @param bucketName:
	 *            the name of the target bucket within the client's S3 storage
	 * @param objectName:
	 *            the name of the target object (file) within the target bucket and
	 *            S3 storage
	 * @param objectSize:
	 *            the size of the target file
	 */
	public ThreadedMultipartDownloadInputStream(AmazonS3Client amazonS3Client, String bucketName, String objectName,
			long objectSize) {

		listOfThreads = new LinkedList<PartDownloadThread>();
		int numberOfParts = 0;
		if (objectSize % PART_SIZE == 0) // defining the number of parts within the target object
			numberOfParts = (int) (objectSize / PART_SIZE);
		else
			numberOfParts = (int) (objectSize / PART_SIZE + 1);
		this.threadPool = Executors.newFixedThreadPool(NUM_THREADS); // maximum number of running threads in parallel

		long startOfRange = 0;
		long endOfRange = 0;
		for (int i = 0; i < numberOfParts; i++) { // creating threads and setting the parameters for them
			startOfRange = PART_SIZE * i;
			endOfRange = startOfRange + PART_SIZE - 1;
			if (endOfRange > objectSize - 1) { // adjust end of the file
				endOfRange = objectSize - 1;
			}
			// PartDownloadThread is an inner class, see below
			PartDownloadThread partDownloadThread = new PartDownloadThread(amazonS3Client, bucketName, objectName,
					startOfRange, endOfRange, parts[i % NUM_THREADS]); // modulo
			threadPool.submit(partDownloadThread); // number of threads = number of parts
			listOfThreads.add(partDownloadThread);
		}
	}

	@Override
	public int read() throws IOException {
		throw new IOException("Not supported");
	}

	/*
	 * Description: This method returns the number of bytes within a part which were
	 * read It returns with -1 if there are no more threads (and parts)
	 * 
	 * (non-Javadoc)
	 * 
	 * @see java.io.InputStream#read(byte[])
	 */
	@Override
	public int read(byte[] buffer) throws IOException {
		do {
			PartDownloadThread t = listOfThreads.get(0); // first thread in the list
			int resultOfReadAvailableBytes = t.readAvailableBytes(buffer); // number of readed bytes //read can throw
																			// exception
			if (resultOfReadAvailableBytes == -1) { // end of reading one part of an object
				t.partReadCountDownLatch.countDown(); // release part
				listOfThreads.remove(0);
			} else
				return resultOfReadAvailableBytes;
		} while (!listOfThreads.isEmpty()); // no more threads
		return -1;
	}

	/*
	 * Description: This method closes the threadPool, annd shuts down all of the
	 * running thrads (non-Javadoc)
	 * 
	 * @see java.io.InputStream#close()
	 */
	@Override
	public void close() throws IOException {
		threadPool.shutdown(); // shutdown ExecutorService
	}

	/**
	 * Description: this is an inner class within the MultipartDownloadInputStream
	 * This call will be called if a new MultipartDownloadInputStream object is
	 * created class
	 */
	static class PartDownloadThread implements Runnable {

		private AmazonS3Client amazonS3Client;
		private String bucketName, fileName;
		private long startOfRange, endOfRange;
		private byte[] part;
		private int writePosition = 0, readPosition = 0; // not read bytes starts from here
		private IOException exception = null;

		private CountDownLatch partDownloadedCountDownLatch = new CountDownLatch(1);
		private CountDownLatch partReadCountDownLatch = new CountDownLatch(1);

		/**
		 * This is the constructor of the PartDownloadThread class within the
		 * MultipartDownloadInputStream class
		 * 
		 * @param amazonS3Client:
		 *            the owner of the target S3 storage
		 * @param bucketName:
		 *            the name of the target bucket within the client's S3 storage
		 * @param objectName:
		 *            the name of the target object (file) within the target bucket
		 * @param startOfRange:
		 *            this is the position where the target part starts
		 * @param endOfRange:
		 *            this is the position where the target part ends
		 * @param part:
		 *            inner part array
		 */
		PartDownloadThread(AmazonS3Client amazonS3Client, String bucketName, String objectName, long startOfRange,
				long endOfRange, byte[] part) {

			this.amazonS3Client = amazonS3Client;
			this.bucketName = bucketName;
			this.fileName = objectName;
			this.startOfRange = startOfRange;
			this.endOfRange = endOfRange;
			this.part = part;
		}

		/*
		 * Description: This method downloads one part of an object from startOfRange to
		 * endOfRange to the part array (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		public void run() {
			// System.out.println("Started downloading part from: " + startOfRange + " to: "
			// + endOfRange);
			GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, fileName);
			rangeObjectRequest.setRange(startOfRange, endOfRange);
			S3Object objectPortion = amazonS3Client.getObject(rangeObjectRequest);
			InputStream in = objectPortion.getObjectContent(); // input in the range
			byte[] buffer = new byte[BUFFER_SIZE];
			writePosition = 0;
			int readBytes = 0; // how many data is in the buffer
			try {
				while ((readBytes = in.read(buffer)) != -1) {
					System.arraycopy(buffer, 0, part, writePosition, readBytes); // copiing
					writePosition += readBytes;
				}
			} catch (IOException e) {
				this.exception = e; // file cannot be read
			}
			try {
				in.close(); // closing inputstream
			} catch (IOException e) {
				e.printStackTrace();
			}
			partDownloadedCountDownLatch.countDown(); // end of downloading a part of an object

			try {
				if (this.exception == null) {
					partReadCountDownLatch.await();
				}
			} catch (InterruptedException e) {
				e.printStackTrace(); // countDownLatch can be interrupted
			}
		}

		/**
		 * Description: This method copies part array to b buffer array
		 * 
		 * @param b:
		 *            the buffer which will be filled with a part's section
		 * @return the number of available bytes which were downloaded before and should
		 *         be copied
		 * @throws IOException
		 */
		public int readAvailableBytes(byte[] b) throws IOException {

			if (this.exception != null) {
				throw this.exception;
			}

			// readPosition = index of bytes which were not read
			// writePosition = index of all downloaded bytes within the part
			try {
				partDownloadedCountDownLatch.await(); // while downloading a part
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int availableBytes = writePosition - readPosition;
			if (availableBytes == 0)
				return -1;
			else {
				int bytesToReturn = Math.min(b.length, availableBytes); // how many bytes should be read max (buffer
																		// size) or less at the end
				System.arraycopy(part, readPosition, b, 0, bytesToReturn);
				readPosition += bytesToReturn;
				return bytesToReturn;
			}
		}
	}
}