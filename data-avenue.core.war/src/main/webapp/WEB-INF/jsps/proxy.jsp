<%@page import="java.util.*,java.io.*,org.ogf.saga.session.*,org.ogf.saga.context.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %>
<f:requestEncoding value="UTF-8" />
<!DOCTYPE html>
<html lang="en">
<head>
	<title>Proxy</title>
	<link rel="stylesheet" type="text/css" href="../css/main.css">
</head>
<body>

<h1>Create proxy (plain/VOMS extended)</h1>

<form method="post" enctype="multipart/form-data">
	<p><label for="usercert">Usercert (usercert.pem):</label><input type="file" id="usercert" name="usercert" /></p>
	<p><label for="userkey">Userkey (userkey.pem):</label><input type="file" id="userkey" name="userkey" /></p>
	<p><label for="password">Password (for userkey):</label><input type="password" id="password" name="password" /></p>
	<p><label for="lifetime">Lifetime:</label><input type="text" id="lifetime" name="lifetime" value="PT120H" /></p>
	<button type="submit" name="action" value="PROXY" style="width:200px">Create Proxy</button>
	<p><label for="server">VOMS server:</label><input type="text" id="server" name="server" value="voms://grid11.kfki.hu:15000/C=HU/O=NIIF CA/OU=GRID/OU=KFKI/CN=grid11.kfki.hu" /></p>
	<p><label for="vo">VO:</label><input type="text" id="vo" name="vo" value="hungrid" /></p>
	<p><label for="vomslifetime">Lifetime:</label><input type="text" id="vomslifetime" name="vomslifetime" value="PT24H" /></p>
	<button type="submit" name="action" value="VOMS" style="width:200px">Create VOMS proxy</button>
</form>

</body>
</html>
