package hu.sztaki.lpds.dataavenue.interfaces;

/**
 * Enum of the possible operation types
 */
public enum OperationsEnum {
	// unary operations
	LIST, // directory listing
	MKDIR, // directory creation
	RMDIR, // directory removal 
	DELETE, // file deletion
	RENAME, // file and directory renaming
	PERMISSIONS, // change file or directory permissions
	INPUT_STREAM, // read file input stream   
	OUTPUT_STREAM, // read file output stream
	
	// binary operations
	COPY_FILE, // file copy (if copy - between adaptor supported protocols - is supported by the same adaptor)
	MOVE_FILE, // file move (if move - between adaptor supported protocols - is supported by the same adaptor)
	COPY_DIR, // directory copy (if copy - between adaptor supported protocols - is supported by the same adaptor)
	MOVE_DIR //directory move (if move - between adaptor supported protocols - is supported by the same adaptor)
}
