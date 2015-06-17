package org.sagebionetworks;

import static org.sagebionetworks.ALSStratificationChallenge.executeDockerCommand;

import java.io.File;

import org.junit.Test;

import com.google.common.io.Files;

public class TestJavaToDocker {

	@Test
	public void testExecuteDockerCommand() throws Exception {
		File tempDir = Files.createTempDir();
		System.out.println(executeDockerCommand(new String[]{"run", "hello-world"}, tempDir));
	}

}
