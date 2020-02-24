<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*,hu.sztaki.lpds.dataavenue.interfaces.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Active transfers</title>
    <link rel="stylesheet" type="text/css" href="./css/main.css">
    <meta http-equiv="refresh" content="10">
</head>
<body>
    <h1>Active</h1>
    <%
    List<ExtendedTransferMonitor> transfers = ExtendedTransferMonitor.getActiveTransfers();
    %>
    <table class="alter">
        <tr>
            <th>Started @</th>
            <th>Task ID</th>
            <th>Server ID</th>
            <th>Status</th>
            <th>Source</th>
            <th>Target</th>
        </tr>
    <%
    for (ExtendedTransferMonitor t: transfers) {
    %>
        <tr>
            <td><%=Utils.dateString(t.getCreated())%></td>
            <td><%=t.getTaskId().substring(0,19)%><br/><%=t.getTaskId().substring(19)%></td>
            <td><%=t.getInstanceId().substring(0,8)%></td>
            <td bgcolor="green"><%=t.getState().name()%></td>
            <td><%=t.getSource()%></td>
            <td><%=t.getDestination()%></td>
        </tr>
    <%
    } // end for
    %>    
    </table>

    <h1>Completed</h1>
    <%
    transfers = ExtendedTransferMonitor.getCompletedTransfers();
    %>
    <table class="alter">
        <tr>
            <th>Duration</th>
            <th>Task ID</th>
            <th>Server ID</th>
            <th>Status</th>
            <th>Source</th>
            <th>Target</th>
            <th>Bytes transferred</th>
        </tr>
    <%
    for (ExtendedTransferMonitor t: transfers) {
    %>
        <tr>
            <td><%=Utils.dateString(t.getStarted())%> -<br/> <%=Utils.dateString(t.getEnded())%></td>
            <td><%=t.getTaskId().substring(0,19)%><br/><%=t.getTaskId().substring(19)%></td>
            <td><%=t.getInstanceId().substring(0,8)%></td>
            <td <%=(TransferStateEnum.DONE == t.getState() || TransferStateEnum.CANCELED == t.getState() ? "" : "bgcolor=\"red\"")%>"><%=t.getState().name()%> <%=t.getFailureCause() != null ? t.getFailureCause() : ""%></td>
            <td><%=t.getSource()%></td>
            <td><%=t.getDestination()%></td>
            <td><%=t.getBytesTransferred()%> / <%=t.getTotalDataSize()%> (<%=(float)t.getBytesTransferred()/((t.getEnded()-t.getStarted())*1000)%> MBs)</td>
        </tr>
    <%
    } // end for
    %>    
    </table>

</body>
</html>