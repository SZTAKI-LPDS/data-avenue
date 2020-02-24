<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Task Details</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>
<h1>Task Details</h1>
<table class="alter">
	<tr>
		<th>Created</th>
		<th>Operaion</th>
		<th>Status</th>
		<th>Source</th>
		<th>Destination</th>
		<th>Progress/size</th>

		<th>Started</th>
		<th>Ended</th>
		
		<th>Failure</th>
		<th>Id</th>
		<th>Ticket</th>
	</tr>
	
	<c:forEach var="item" items="${tranfermonitors}">
	<tr>
		<td>${item.createdString}</td>
		<td>${item.operation}</td>
		<td>${item.state}</td>
		<td>${item.source}</td>
		<td>${item.destination}</td>
		<td><c:out value="${item.bytesTransferred > 0 ? item.bytesTransferred: '?'}"/> / <c:out value="${item.totalDataSize > 0 ? item.totalDataSize: '?'}"/> bytes </td> 

		<td>${item.startedString}</td>
		<td>${item.endedString}</td>
		
		<td>${item.failureCause}</td>
		<td>${item.taskId}</td>
		<td>${item.ticket}</td>

	</tr>
	</c:forEach>
</table>
</body>
</html>