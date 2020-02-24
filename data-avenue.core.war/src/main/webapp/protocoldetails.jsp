<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html>
<head>
	<title>Protocol Details</title>
	<link rel="stylesheet" type="text/css" href="css/dataavenue.css">
</head>

<h1>Protocol Adaptor Details</h1>
<div>
<table class="alter">
<tr><th>Protocol</th><td><b>${protocol}</b></td>
<tr><th>Adaptor name</th><td>${adaptorname}</td>
<tr><th>Version</th><td>${adaptorversion}</td>
<tr><th>Adaptor description</th><td>${adaptordescription}</td>
<tr><th>Supported operations</th><td>${adaptoroperations}</td>
<tr><th>Adaptor class</th><td>${adaptorclass}</td>
</table>
</div>

<h1>Authentication Types and Usage</h1>
<div>
<table class="alter">
<tr><th width="20%">Authentication Type</th><th>Required Credential Data</th></tr>

<c:forEach var="item" items="${authtypesandusage}">
	<tr><td>${item.key}</td><td>${item.value}</td></tr>
</c:forEach>
</table>
</div>
<p><a href="index.jsp">Back to Supported Protocols</a></p>
</body>
</html>
