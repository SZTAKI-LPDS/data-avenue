<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Configuration</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>

<h1>Configuration</h1>
<form>
<table class="alter">
	<tr>
		<th>Accept commands:</th>
		<c:choose>
			<c:when test="${acceptCommands}">
				<td><font color="green"><b>enabled</b></font></td>
				<td><button type="submit" name="action" value="DISABLE_ACCEPT_COMMANDS">Disable</button></td>
			</c:when>
			<c:otherwise>
				<td><font color="red"><b>disabled</b></font></td>
				<td><button type="submit" name="action" value="ENABLE_ACCEPT_COMMANDS">Enable</button></td>
			</c:otherwise>
		</c:choose>
	</tr>
	<tr>
		<th>Accept copy commands:</th>
		<c:choose>
			<c:when test="${acceptCopyCommands}">
				<td><font color="green"><b>enabled</b></font></td>
				<td><button type="submit" name="action" value="DISABLE_ACCEPT_COPY_COMMANDS">Disable</button></td>
			</c:when>
			<c:otherwise>
				<td><font color="red"><b>disabled</b></font></td>
				<td><button type="submit" name="action" value="ENABLE_ACCEPT_COPY_COMMANDS">Enable</button></td>
			</c:otherwise>
		</c:choose>
	</tr>
	<tr>
		<th>Accept HTTP aliases:</th>
		<c:choose>
			<c:when test="${acceptHttpAliases}">
				<td><font color="green"><b>enabled</b></font></td>
				<td><button type="submit" name="action" value="DISABLE_ACCEPT_HTTP_ALIASES">Disable</button></td>
			</c:when>
			<c:otherwise>
				<td><font color="red"><b>disabled</b></font></td>
				<td><button type="submit" name="action" value="ENABLE_ACCEPT_HTTP_ALIASES">Enable</button></td>
			</c:otherwise>
		</c:choose>
	</tr>
	<tr>
		<th>Credentials encryption key:</th>
		<td><input type="text" name="key" value="<c:out value="${key}"/>"/></td>
		<td><button type="submit" name="action" value="SET_HTTP_ALIAS_ENCRYPTION_KEY">Set</button></td>
	</tr>
</table>
</form>
</body>
</html>