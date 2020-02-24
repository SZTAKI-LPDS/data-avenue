<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %><f:requestEncoding value="UTF-8" /><!DOCTYPE html>
<html lang="en">
<head>
	<title>Create key</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>

<h1>Create access key</h1>
<font color="red"><c:out value="${error}"></c:out></font>

<form method="post">
<table class="alter">
	<tr>
		<th>Key:</th>
		<td><input type="text" name="id" size="35" value="<c:out value="${id}"><%=UUID.randomUUID()%></c:out>"/></td>
	</tr>
	<tr>
		<th>Name:</th>
		<td><input type="text" name="name" value="<c:out value="${name}"></c:out>" size="40" maxlength="255" placeholder="Full name"/></td>
	</tr>
	<tr>
		<th>Company:</th>
		<td><input type="text" name="company" value="<c:out value="${company}"></c:out>" size="30" maxlength="255" placeholder="Company/organization name"/></td>
	</tr>
	<tr>
		<th>E-mail (required):</th>
		<td><input type="email" name="email" value="<c:out value="${email}"></c:out>" size="30" maxlength="255" placeholder="E-mail address" required/></td>
	</tr>
	<tr>
		<th>Type:</th>
		<td><select name="type"><option value="admin" <c:if test="${type == 'admin'}">selected</c:if>>Portal administrator</option><option value="api" <c:if test="${type == 'api'}">selected</c:if>>API user</option></select>
	</tr>
	<tr>
		<th></th>
		<td><button type="submit" name="action" value="create">Create</button></td>
	</tr>
</table>
</form>
</body>
</html>