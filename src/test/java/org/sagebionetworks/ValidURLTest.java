package org.sagebionetworks;

import static org.junit.Assert.*;
import static org.sagebionetworks.ALSStratificationChallenge.isValidDockerReference;

import org.junit.Test;

public class ValidURLTest {

	@Test
	public void testValidURL() throws Exception {
		// valid
		assertTrue(isValidDockerReference("/user_name/model_name@sha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));

		// hash not right
		assertFalse(isValidDockerReference("/user_name/model_name@sha256:da4"));
		assertFalse(isValidDockerReference("/user_name/model_name@sha256:dA42E2F2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));

		// @sha256: missing
		assertFalse(isValidDockerReference("/user_name/model_name@sha:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));
		assertFalse(isValidDockerReference("/user_name/model_namesha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));
		assertFalse(isValidDockerReference("/user_name/model_name@sha256da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));

		// username not right
		assertFalse(isValidDockerReference("//model_name@sha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));
		assertFalse(isValidDockerReference("/user#name/model_name@sha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));

		// repo/model name not right
		assertFalse(isValidDockerReference("/user_name/@sha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));
		assertFalse(isValidDockerReference("/user_name/model&name@sha256:da42e2f2b3e5c4955d2bca83428409aed7eed4b8f514a369a0a792fc36b47b23"));
	}

}
