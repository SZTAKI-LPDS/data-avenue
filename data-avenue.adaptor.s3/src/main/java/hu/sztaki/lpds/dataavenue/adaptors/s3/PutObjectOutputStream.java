package hu.sztaki.lpds.dataavenue.adaptors.s3;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

/*
 * - up to 5 GB in size
 * - not applicable for streaming data of unknown size:
 * "When uploading directly from an input stream, content length must be specified before data can be uploaded to Amazon S3. 
 * If not provided, the library will have to buffer the contents of the input stream in order to calculate it. 
 * Amazon S3 explicitly requires that the content length be sent in the request headers before any of the data is sent."
 */

public class PutObjectOutputStream {
	public static long PUT_OBJECT_LIMIT = 10000000l; // 10 MB

	private static final Logger log = LoggerFactory.getLogger(PutObjectOutputStream.class);
	public static PutObjectResult writeInputStream(AmazonS3Client client, final String bucketName, final String keyName, final InputStream in, final long contentLength) {
		log.trace("New PutObjectOutputStream (content-length: " + contentLength + ")");
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(contentLength);
		return client.putObject(new PutObjectRequest(bucketName, keyName, in, meta));
	}
}