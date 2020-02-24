package hu.sztaki.lpds.dataavenue.core;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class Utils {
	public static String dateString(final Long time) {
		if (time == null || time == 0l) return "-";
		else return new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss").format(new Date(time));
	}

	public static String getExceptionTrace(Throwable e) {
		// no message, no cause, return ""
		if ((e.getMessage() == null || "".equals(e.getMessage())) && e.getCause() == null) return "";
		
		StringBuilder result = new StringBuilder();
		result.append(" (");
		if (e.getMessage() != null && !"".equals(e.getMessage())) {
			result.append(e.getMessage());
		}
		
		Throwable causeException = e.getCause();
		if (causeException != null) {
			result.append(" caused by: ");
			while (causeException != null) {
				result.append(causeException.getMessage());
				causeException = causeException.getCause();
				if (causeException != null) result.append(", ");
			}
		}
		result.append(")");
		return result.toString();
	}

	
	public static String getExceptionStringWithOneCause(Throwable e) {
		// no message, just return cause
		if (e == null || ((e.getMessage() == null || "".equals(e.getMessage())) && e.getCause() == null)) return "Unknown exception";
		
		StringBuilder result = new StringBuilder();
		if (e.getMessage() != null && !"".equals(e.getMessage())) {
			result.append(e.getMessage());
		} else result.append("Exception");
		
		Throwable causeException = e.getCause();
		if (causeException != null) {
			result.append(" (caused by: ");
			result.append(causeException.getMessage());
			result.append(")");
		}
		return result.toString();
	}	
}