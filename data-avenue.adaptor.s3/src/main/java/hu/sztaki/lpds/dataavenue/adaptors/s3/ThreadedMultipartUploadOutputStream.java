package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.UploadPartRequest;

/*
 * WARNING: This class is not thread-safe. Use MultipartUploadOutputStream from one thread!
 * Memory usage of one copy: max. MAX_UPLOAD_THREADS * PART_SIZE
 */
public class ThreadedMultipartUploadOutputStream extends OutputStream {
	private static final Logger log = LoggerFactory.getLogger(ThreadedMultipartUploadOutputStream.class);
	
	public static int DEFAULT_PART_SIZE = 10 * 1024 * 1024; // default: 10MiB, 5MB<=PART_SIZE<=5GB, max part id=10000 => up to 100GB
	public static int DEFAULT_MAX_UPLOAD_THREADS = 4; // number of upload threads
	
	private int UPLOAD_THREADS; // max. memory requirements: MAX_UPLOAD_THREADS * PART_SIZE
	private int PART_SIZE; // max. memory requirements: MAX_UPLOAD_THREADS * PART_SIZE
	
	protected final AmazonS3Client s3Client;
	protected final String bucketName;
	protected final String keyName;
	protected String uploadId;
	
	private int partNumber = 1; // 1..10000
	private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>()); // ArrayList<PartETag>();

	private ExecutorService uploadPool; // = Executors.newFixedThreadPool(MAX_UPLOAD_THREADS);
	final private Future<?> slotThreads []; // = new Future<?> [MAX_UPLOAD_THREADS];
	final private byte slotBuffers[][]; // = new byte [MAX_UPLOAD_THREADS][];
	private int currentReadSlot; // one slot is reading others are writing
	private byte currentReadBuffer []; // initial read buffer
	private int currentReadBufferContentLength;
	
	ThreadedMultipartUploadOutputStream(final AmazonS3Client s3Client, final String bucketName, final String keyName, final int partSize, final int numberOfThreads, final long contentLength) throws OperationException {
		log.trace("New ThreadedMultipartUploadOutputStream (part size: " + partSize + ", threads: " + numberOfThreads + ")");
		UPLOAD_THREADS = numberOfThreads;
		PART_SIZE = partSize;
//		log.trace("Creating threaded multipart S3 upload (part size: {}, threads: {})...", PART_SIZE, UPLOAD_THREADS);
		this.uploadPool = Executors.newFixedThreadPool(UPLOAD_THREADS);
		this.slotThreads = new Future<?> [UPLOAD_THREADS];
		this.slotBuffers = new byte [UPLOAD_THREADS][];
		
		this.s3Client = s3Client;
		this.bucketName = bucketName;
		this.keyName = keyName;
		try {
	        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
	        if (contentLength >= 0) {
		        ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(contentLength);
				initRequest.withObjectMetadata(meta);
	        }
	        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
	        this.uploadId = initResponse.getUploadId();
//			log.trace("MultipartUploadOutputStream created: " + uploadId);
		}  catch (Throwable e) {
			log.error("Cannot initiate multipart upload!");
			abort(e);
			throw new OperationException(e);
		}
		
		// set initial slot, and allocate buffer
		currentReadSlot = 0;
		slotBuffers[currentReadSlot] = new byte [PART_SIZE];
		currentReadBufferContentLength = 0;
		currentReadBuffer = slotBuffers[currentReadSlot];
	}
	
	@Override public void write(byte b[], int off, int len) throws IOException { // NOTE: does not check parameters
//		assert (len <= PART_SIZE): "Buffer cannot be greater than part size"; // so if buffer does not fit into part, the remaining will fit into the next one  

		if (currentReadBufferContentLength + len < PART_SIZE) { // buffer fits into part
			System.arraycopy(b, off, currentReadBuffer, currentReadBufferContentLength, len);
			currentReadBufferContentLength += len;
		} else { // read chunk of buffer, then write
			try {
				final int consumedLen = PART_SIZE - currentReadBufferContentLength; // consumed bytes = free space left in part buffer
				System.arraycopy(b, off, currentReadBuffer, currentReadBufferContentLength, consumedLen);
				currentReadBufferContentLength += consumedLen; // must be equal to PART_SIZE
				
//				assert(currentReadBufferContentLength == PART_SIZE);
				writePart(partNumber++, currentReadBuffer, currentReadBufferContentLength);
				currentReadBuffer = slotBuffers[this.currentReadSlot];
				if (currentReadBuffer == null) currentReadBuffer = slotBuffers[this.currentReadSlot] = new byte [PART_SIZE];
				currentReadBufferContentLength = 0;

				final int remainingLen = len - consumedLen;
				if (remainingLen > 0) {
					final int remainingOffset = off + consumedLen;
					System.arraycopy(b, remainingOffset, currentReadBuffer, currentReadBufferContentLength, remainingLen);
					currentReadBufferContentLength += remainingLen;
				}
			
			}  catch (Throwable e) {
				log.error("Cannot write multipart upload!");
				abort(e);
				throw new IOException("Cannot write multipart upload!", e);
			}
		} 
	}
	
	// don't use it 
	@Override public void write(int b) throws IOException { // no need to synchronize, one thread writes this stream, and blocks until write returns
		try {
			if (currentReadBufferContentLength == PART_SIZE) { // flush: upload part
				writePart(partNumber++, currentReadBuffer, currentReadBufferContentLength);
				currentReadBuffer = slotBuffers[this.currentReadSlot];
				if (currentReadBuffer == null) currentReadBuffer = slotBuffers[this.currentReadSlot] = new byte [PART_SIZE];
				currentReadBufferContentLength = 0;
			} 
			currentReadBuffer[currentReadBufferContentLength++] = (byte) b;
		}  catch (Throwable e) {
			log.error("Cannot write multipart upload!");
			abort(e);
			throw new IOException("Cannot write multipart upload!", e);
		}
	}

	@Override public void close() throws IOException {
//		log.trace("Closing multipart upload...");
		
		// flush buffer if it contains any data
		try {
			if (currentReadBufferContentLength > 0) writePart(partNumber++, currentReadBuffer, currentReadBufferContentLength); // returned buffer not needed
		}  catch (Throwable e) {
			log.error("Cannot write multipart upload!");
			abort(e);
			throw new IOException("Cannot write multipart upload!", e);
		}
		
		// wait for upload tasks completion
		uploadPool.shutdown();
		
		// TODO check thread exceptions
		
		try { uploadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); } 
		catch (InterruptedException e) { System.out.println("Can't wait... (" + e.getMessage() + ")"); }
		
		// complete multipart upload
		try {
//			log.trace("Finalizing multipart upload...");
	        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName, uploadId, partETags);
	        s3Client.completeMultipartUpload(compRequest);
		}  catch (Throwable e) {
			log.error("Cannot close multipart upload!");
			abort(e);
			throw new IOException("Cannot close multipart upload! (Data size exceeds limit?)", e);
		}
		
		// release allocated buffers
		System.gc(); 
		log.trace("Multipart upload complete");
	}
	
	/*
	 * 	modifies currentReadSlot index
	 */
	private void writePart(final int partNumber, final byte[] writeBuffer, final int contentLength) throws Exception {
		// upload part in thread and add response to our list
		if (partNumber % 10 == 0) log.debug("Writing part {} (slot {})", partNumber, currentReadSlot + 1);
		slotThreads[currentReadSlot] = uploadPool.submit(new ThreadedPartUpload(this, partNumber, writeBuffer, contentLength, partETags));
		
		// search for free slot for new current
		this.currentReadSlot = -1;
		do {
			for (int i = 0; i < slotThreads.length; i++) {
				if (slotThreads[i] == null || slotThreads[i].isDone()) { // free or completed thread
//					if (slotThreads[i] != null) try { slotThreads[i].get(); } catch (Exception e) { throw new Exception(e); } // check thread exception
					currentReadSlot = i;
					slotThreads[i] = null;
					break;
				}
			}
			if (this.currentReadSlot == -1) { // wait 0.1s, until any of the threads finishes
				try { Thread.sleep(100); } catch (Exception e) {}
			}
		} while (this.currentReadSlot == -1);
	} 

//	private void checkThreadExceptions() throws Exception {
//		for (int i = 0; i < slotThreads.length; i++) {
//			if (slotThreads[i] != null) {
//				try { slotThreads[i].get(); }
//				catch (Exception e) { throw new Exception(e); }
//			}
//		}
//	}
	
	private void abort(Throwable e) { // on any exception
		log.error("Aborting upload output stream: " + bucketName + "/" + keyName, e);
		if (s3Client != null && uploadId != null) {
			try { s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadId)); } 
			catch (Throwable x) { log.debug("Cannot abort!", x); }
		}
		// release executor pool
		if (uploadPool != null) {
			uploadPool.shutdownNow();
			uploadPool = null;
		}
	}
	
	private static class ThreadedPartUpload implements Runnable { 

		final ThreadedMultipartUploadOutputStream parent;
		final int partNumber; 
		final byte[] buffer; 
		final int contentLength;
		final List<PartETag> partETags;
		
		ThreadedPartUpload(final ThreadedMultipartUploadOutputStream parent, final int partNumber, final byte[] buffer, final int contentLength, final List<PartETag> partETags) {
			this.parent = parent;
			this.partNumber = partNumber;
			this.buffer = buffer; // java.util.Arrays.copyOf(buffer, contentLength);
			this.contentLength = contentLength;
			this.partETags = partETags;
		}

		@Override public void run() {
			try {
					
				UploadPartRequest uploadRequest = new UploadPartRequest()
			  	.withBucketName(parent.bucketName)
			  	.withKey(parent.keyName)
			  	.withUploadId(parent.uploadId)
			  	.withPartNumber(partNumber)
			  	.withInputStream(new ByteArrayInputStream(buffer, 0, contentLength))
			  	.withPartSize(contentLength);
				
				PartETag partETag = parent.s3Client.uploadPart(uploadRequest).getPartETag();
				partETags.add(partETag);
				if (partNumber % 10 == 0) log.debug("Part " + partNumber + " done");
			
			} catch (Throwable e) {
				log.error(e.getMessage(), e);
			}
		}
	}
}