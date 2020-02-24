package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.Configuration;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.ws.rs.core.Response.Status;

@Path("/version")
public class Version {
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public Response getAttributes(@HeaderParam(CustomHttpHeaders.HTTP_HEADER_DETAILS) String details) {
		String responseString = Configuration.getVersion();
		if (details != null) {
			int cores = Runtime.getRuntime().availableProcessors();
			long heapSize = Runtime.getRuntime().totalMemory();
			long heapMaxSize = Runtime.getRuntime().maxMemory();
			long heapFreeSize = Runtime.getRuntime().freeMemory(); 
			String info = "(CPU cores: " + cores + ", heap max: " + (heapMaxSize / 1000000) + "MB, heap current: " + (heapSize  / 1000000) + "MB, heap free: " + (heapFreeSize / 1000000) + "MB)";
			responseString += " " + info;
		}
		return Response.status(Status.OK).entity(responseString).build();
	}
}