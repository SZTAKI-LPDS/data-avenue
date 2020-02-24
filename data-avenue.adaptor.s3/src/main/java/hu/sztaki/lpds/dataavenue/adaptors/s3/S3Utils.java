package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public final class S3Utils {
	
	static String getRelativePath(final String parent, final String child) {
	    if (!child.startsWith(parent)) {
	        throw new IllegalArgumentException("Invalid child '" + child 
	            + "' for parent '" + parent + "'");
	    }
	    // a String.replace() also would be fine here
	    int parentLen = parent.length();
	    return child.substring(parentLen);
	}

	static boolean isImmediateDescendant(final String parent, final String child) {
	    if (!child.startsWith(parent)) {
	        // maybe we just should return false
	        throw new IllegalArgumentException("Invalid child '" + child 
	            + "' for parent '" + parent + "'");
	    }
	    int parentLen = parent.length();
	    String childWithoutParent = child.substring(parentLen);
	    if (childWithoutParent.contains("/")) return false;
	    return true;
	}
	
	public static void printDir(List<URIBase> dir) {
		System.out.println("Directory contents: ");
		if (dir.size() == 0) { System.out.println("  empty"); return; }
		else for (URIBase i: dir) {
			StringBuilder entry = new StringBuilder();
			entry.append("  ");
			entry.append(i.getEntryName());
			
			if (i.getType() == URIBase.URIType.DIRECTORY) entry.append("/"); 
			
			if (i.getPermissions() != null) entry.append(" " + i.getPermissions());
			else entry.append(" ?");
			
			if (i.getLastModified() != null) entry.append(" " + toDate(i.getLastModified()));
			else entry.append(" ?");

			if (i.getSize() != null) entry.append(" " + i.getSize() + "B");
			else entry.append(" ?B");
			
			if (i.getDetails() != null) entry.append(" " + i.getDetails()); 
			System.out.println(entry);
		}
	}
	static String toDate(Long time) {
		if (time == null) return "?";
		return new SimpleDateFormat("dd-MMM-yyyy HH:mm").format(new Date(time));
	}
}
