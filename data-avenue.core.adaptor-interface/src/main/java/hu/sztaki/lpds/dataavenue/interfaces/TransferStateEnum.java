package hu.sztaki.lpds.dataavenue.interfaces;

public enum TransferStateEnum {
	CREATED, 		// task created 	0
	SCHEDULED, 		// POOLED			1
	TRANSFERRING, 	// RUNNING			2
	DONE, 			// COMPLETED		3
	FAILED,   		//					4
	CANCELED, 		// user abort 		5
	ABORTED, 		// server restart 	6
	RETRY; 			// retrying  		7
}