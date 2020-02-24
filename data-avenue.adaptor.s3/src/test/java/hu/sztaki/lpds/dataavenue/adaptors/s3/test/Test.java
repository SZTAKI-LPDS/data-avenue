package hu.sztaki.lpds.dataavenue.adaptors.s3.test;

import hu.sztaki.lpds.dataavenue.adaptors.s3.CredsImpl;
import hu.sztaki.lpds.dataavenue.adaptors.s3.S3Adaptor;
import hu.sztaki.lpds.dataavenue.adaptors.s3.S3URIImpl;
import hu.sztaki.lpds.dataavenue.adaptors.s3.S3Utils;

@SuppressWarnings("unused")
public class Test {
	
//	@SuppressWarnings("unused")
	public static void main(String args []) throws Exception {
		System.setProperty("java.util.logging.config.file", "d:/Java/workspace/data-avenue-amazon-s3-adaptor/commons-logging.properties");
		System.setProperty("log4j.configuration", "d:/Java/workspace/data-avenue-amazon-s3-adaptor/log4j.properties");
		
		S3Adaptor adaptor = new S3Adaptor();
		CredsImpl creds = new CredsImpl();
		creds.putCredentialAttribute("UserID", "user");
		creds.putCredentialAttribute("UserPass", "***");
		
//		list tests:
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu:443"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu:443/"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/myfirstbucket"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/myfirstbucket/dir/"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/myfirstbucket/ckos/"), creds, null)); // not existing
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/"), creds, null));
//		S3Utils.printDir(adaptor.list(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/"), creds, null));

//		mkdir tests:
//		adaptor.mkdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket"), creds, null);
//		adaptor.mkdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket"), creds, null);
		
//		adaptor.permissions(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket"), creds, null, "------rw-");
		
//		rmdir tests:
//		adaptor.rmdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/dir/"), creds, null);
//		adaptor.rmdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/"), creds, null);

//		adaptor.mkdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket//test/"), creds, null);
//		adaptor.rmdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/dir/test/"), creds, null);
//		adaptor.rmdir(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/dir/test/"), creds, null);
		
//		delete tests:
//		adaptor.delete(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/12_3.bmp"), creds, null);
		
//		rename tests:
//		adaptor.rename(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/f/12_3.bmp"), "newname.bmp", creds, null);
//		adaptor.rename(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/"), "d/", creds, null);
		
//		input stream tests:
//		InputStream is = adaptor.getInputStream(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/new.bmp"), creds, null);
//		int bytes = 0, read = 0; byte [] buffer = new byte[16000]; while((read = is.read(buffer)) > 0) bytes+=read; is.close();
//		System.out.println("bytes: " + bytes);

//		output stream tests:
//		OutputStream os = adaptor.getOutputStream(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/out.txt"), creds, null);
//		OutputStreamWriter out = new OutputStreamWriter(os); out.write("This"); out.close();
		
//		file size tests:
//		long size = adaptor.getFileSize(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/out.txt"), creds, null);
		
//		readable, writable tests:
//		System.out.println("readable: " + adaptor.isReadable(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/out3.txt"), creds, null));
//		System.out.println("writable: " + adaptor.isWritable(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/g/b/out2.txt"), creds, null));
//		System.out.println("readable: " + adaptor.isReadable(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/newfolder/newtestfolder_renamed/blacktop-2.0.0.war"), creds, null));
//		System.out.println("writable: " + adaptor.isWritable(new S3URIImpl("s3://s3.lpds.sztaki.hu/testbucket/newfolder/newtestfolder_renamed/blacktop-2.0.0.war"), creds, null));
	}
}
