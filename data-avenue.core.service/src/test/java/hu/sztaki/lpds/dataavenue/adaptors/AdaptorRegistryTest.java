package hu.sztaki.lpds.dataavenue.adaptors;

import static org.junit.Assert.*;
import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdaptorRegistryTest {

	@Before
	public void beforeEach() {}
	
	@After
	public void afterEach() {}
	
	@Test
	public void test1() throws NotSupportedProtocolException { // http adaptor registered test
		assertNotNull(AdaptorRegistry.getAdaptorInstance("http"));
	}

}
