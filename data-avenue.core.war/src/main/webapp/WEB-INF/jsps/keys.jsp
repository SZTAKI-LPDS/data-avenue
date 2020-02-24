<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Keys</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>
<h1>Access keys</h1>
<table class="alter">
	<tr>
		<th>Id</th>
		<th>Enabled</th>
		<th>Name</th>
		<th>Company</th>
		<th>E-mail</th>
		<th>Created</th>
		<th>ValidThru</th>
		<th>Role</th>
		<th>Parent</th>
		<th>Max sessions</th>
		<th>Max transfers</th>
		<th>Max aliases</th>
	</tr>
	
	<c:forEach var="item" items="${keys}">
	<tr>
		<td>${item.ticket}</td>
		<td><c:out value="${item.disabled ? 'disabled': 'enabled'}"/></td>
		<td><c:out value="${item.name}">-</c:out></td>
		<td><c:out value="${item.company}">-</c:out></td>
		<td>${item.email}</td>
		
		<td>${item.createdString}</td>
		<td>${item.validThruString}</td>
		
		<td>${item.ticketType}</td>
		<td>${item.parent}</td>

		<td>${item.maxSessions}</td>
		<td>${item.maxTransfers}</td>
		<td>${item.maxAliases}</td>
	</tr>
	</c:forEach>
</table>
</body>
</html>