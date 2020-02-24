<%@page import="java.util.*,java.io.*,hu.sztaki.lpds.dataavenue.core.*" %><%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %><%@taglib prefix="f" uri="http://java.sun.com/jsp/jstl/fmt" %>
<f:requestEncoding value="UTF-8" />
<!DOCTYPE html>

<html>

<head>
  <title>Load monitor</title>

  <!--Load the AJAX API-->
  <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
  <script type="text/javascript">


    // Load the Visualization API and the corechart package.
    google.charts.load('current', { 'packages': ['corechart', 'bar'] });

    // Set a callback to run when the Google Visualization API is loaded.
    google.charts.setOnLoadCallback(drawChart);

    // Callback that creates and populates a data table,
    // instantiates the pie chart, passes in the data and
    // draws it.
    function drawChart() {
      // Create the data table.
      var data = new google.visualization.DataTable();
      data.addColumn('string', 'DataAvenue server name');
      data.addColumn('number', 'Number of active transfers');
      data.addRows([
<%
      List < Object[] > instancesLoad = Instances.getInstancesLoad();
      for (int i = 0; i < instancesLoad.size(); i++) {
        Object[] data = instancesLoad.get(i);
%>
          ['<%=data[0]%>', <%=data[2] %>] <%=i < instancesLoad.size() - 1 ? "," : "" %>
<%
}
%>
            ]);

      // Set chart options
      var baroptions = {
        title: 'Load distribution among server instances (bar chart)',
        subtitle: 'Number of active transfers per instances',
	legend: {position: 'none'},
	height: '300',
        hAxis: {
          title: 'Number of active transfers',
          minValue: 0,
          ticks: [0, 5, 10, 15, 20]
        },
        vAxis: {
          title: 'Server instances'
        }
      };

      var pieoptions = {
     	title: 'Load distribution among server instances (pie chart)',
        height: '300',
      };

      // Instantiate and draw our chart, passing in some options.
      var chart = new google.visualization.BarChart(document.getElementById('bar_chart_div'));
      chart.draw(data, baroptions);


      var piechart = new google.visualization.PieChart(document.getElementById('pie_chart_div'));
      piechart.draw(data, pieoptions);
    }
  </script>
</head>

<body>

  <h1 style="font-family: Arial">Load distribution</h1>

  <!--Div that will hold the pie chart-->
  <div id="bar_chart_div"></div>
  <div id="pie_chart_div"></div>
</body>

</html>