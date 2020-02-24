package hu.sztaki.lpds.dataavenue.interfaces;

public class Limits {
	public static int MAX_ERRORS = 3; // retry at most this times
	public static long MAX_BYTES_TO_RETRANSFER = 1024*1024l; // retry at most this many bytes
	public static long ERROR_RETRY_DELAY = 60000l; // sleep before retry on error
}
