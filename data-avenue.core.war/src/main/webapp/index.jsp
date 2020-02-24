<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html>
<%
	String path = "META-INF/maven/hu.sztaki.lpds/dataavenue/pom.properties";
	ServletContext servletContext = getServletConfig().getServletContext(); 
	InputStream in = servletContext.getResourceAsStream(path);
	Properties prop = new Properties();
	try { prop.load(in); } 
	catch (Exception e) {} finally { try { in.close(); } catch (Exception x) {} }
	pageContext.setAttribute("version", Configuration.getVersion());
	pageContext.setAttribute("bytesTransferred", Statistics.getBytesTransferred() / 1048576);
	pageContext.setAttribute("acceptCommands", Configuration.acceptCommands ? "enabled" : "disabled");
	pageContext.setAttribute("acceptCopyCommands", Configuration.acceptCopyCommands ? "enabled" : "disabled");
	pageContext.setAttribute("acceptHttpAliases", Configuration.acceptHttpAliases ? "enabled" : "disabled");
	pageContext.setAttribute("adaptors", AdaptorRegistry.getAdaptorDetails());
	pageContext.setAttribute("authdetails", AdaptorRegistry.getProtocolAuthenticationDetails());
%>
<head>
	<title>Data Avenue Core Services (Blacktop)</title>
	<link rel="stylesheet" type="text/css" href="css/dataavenue.css">
</head>

<body>
<h1>Supported Protocols</h1>
<div>
<table class="alter">
	<tr><th align="left">Protocol</th><th align="left">Adaptor</th><th>Supported Authentication Types</th></tr>
	<c:forEach var="item" items="${adaptors}">
	<tr id="trprotocol" onclick="location.href='protocoldetails?protocol=${item[0]}'" style="cursor:pointer">
	<th id="protocol">${item[0]}</th>
	<td id="adaptor" align="left" title="<c:out value="${item[2]}" />">${item[1]}</td>
	<td align="left">${item[3]}</td>
	</tr>
	</c:forEach>
</table>
</div>

<div style="text-align: center"><i>Click on a protocol for more details about the supported operations and authentication types!</i></div>
<br/>* jSAGA is a Java implementation of the Simple API for Grid Applications (SAGA) specification from the Open Grid Forum (OGF) developed by IN2P3 Computing Center
<!--br/><br/><center>&#169;2014-2019 SZTAKI / Laboratory of Parallel and Distributed Systems</center-->
</body>
</html>