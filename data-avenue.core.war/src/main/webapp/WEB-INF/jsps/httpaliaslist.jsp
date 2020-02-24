<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>HTTP Aliases</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>
<h1>HTTP Aliases</h1>
<table class="alter">
	<tr>
		<th>Created</th>
		<th>Url</th>
		<th>Status</th>
		<th>Expires</th>
		<th>Progress/size</th>
		<th>Read/write</th>
		<th>Failure</th>
		<th>Id</th>
		<th>Ticket</th>
	</tr>
	
	<c:forEach var="item" items="${aliases}">
	<tr>
		<td>${item.createdString}</td>
		<td>${item.source}</td>
		<td>${item.status}</td>
		<td>${item.expiresString}</td>
		<td><c:out value="${item.progress > 0 ? item.progress: '?'}"/> / <c:out value="${item.size > 0 ? item.size: '?'}"/> bytes </td> 
		<td><c:out value="${item.forReading ? 'read': 'write'}"/></td>
		<td><c:out value="${item.failureReason != null ? item.failureReason: '-'}"/></td>
		<td><a href="../${item.aliasId}">${item.aliasId}</a></td>
		<td>${item.ticket}</td>
	</tr>
	</c:forEach>
</table>
</body>
</html>