<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Server instances</title>
    <link rel="stylesheet" type="text/css" href="./css/main.css">
    <meta http-equiv="refresh" content="10">
</head>
<body>
    <h1>Server instances</h1>

    <%
    List<Instances> instances = Instances.getInstances();
    %>
    <table class="alter">
        <tr>
            <th>IP</th>
            <th>ID</th>
            <th>Status</th>
            <th>Lifetime</th>
            <th>Started @</th>
            <th>Died @</th>
        </tr>
    <%
    for (Instances instance: instances) {
    %>
        <tr>
            <td><%=instance.getPrivateIp()%></td>
            <td><%=instance.getInstanceId().substring(0,8)%></td>
            <td bgcolor="<%=InstanceStateEnum.ALIVE == instance.getState() ? "green" : "red"%>"><%=instance.getState().name()%></td>
            <td><%=(instance.getTouched() - instance.getCreated())/1000%>s</td>
            <td><%=Utils.dateString(instance.getCreated())%></td>
            <td><%=Utils.dateString(instance.getDied())%></td>
        </tr>
    <%
    } // end for
    %>    
    </table>
</body>
</html>