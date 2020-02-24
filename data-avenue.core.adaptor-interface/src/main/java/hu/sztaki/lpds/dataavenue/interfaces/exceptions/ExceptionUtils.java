package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

public abstract class ExceptionUtils {
	
	public static String getExceptionTrace(Throwable e) {
		// no message, no cause, return ""
		if ((e.getMessage() == null || "".equals(e.getMessage())) && e.getCause() == null) return "";
		
		StringBuilder result = new StringBuilder();
		result.append(" (");
		if (e.getMessage() != null && !"".equals(e.getMessage())) {
			result.append(e.getMessage());
		}
		
		Throwable causeException = e.getCause();
		if (causeException != null && causeException.equals(e.getMessage())) {
			result.append(", caused by: ");
			while (causeException != null) {
				result.append(causeException.getMessage());
				causeException = causeException.getCause();
				if (causeException != null) result.append(", ");
			}
		}
		result.append(")");
		return result.toString();
	}
}
