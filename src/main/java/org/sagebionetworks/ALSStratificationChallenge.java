package org.sagebionetworks;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.SynapseProfileProxy;
import org.sagebionetworks.client.exceptions.SynapseConflictingUpdateException;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.evaluation.model.BatchUploadResponse;
import org.sagebionetworks.evaluation.model.Evaluation;
import org.sagebionetworks.evaluation.model.Submission;
import org.sagebionetworks.evaluation.model.SubmissionBundle;
import org.sagebionetworks.evaluation.model.SubmissionStatus;
import org.sagebionetworks.evaluation.model.SubmissionStatusBatch;
import org.sagebionetworks.evaluation.model.SubmissionStatusEnum;
import org.sagebionetworks.reflection.model.PaginatedResults;
import org.sagebionetworks.repo.model.annotation.Annotations;
import org.sagebionetworks.repo.model.annotation.StringAnnotation;
import org.sagebionetworks.repo.model.file.ExternalFileHandle;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.PreviewFileHandle;
import org.sagebionetworks.repo.model.message.MessageToUser;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;

import com.google.common.io.Files;


/**
 * Scoring framework for ALS Stratification Challenge
 * 
 * Validation:
 * docker pull <submission>
 * 	if it fails, return the shell output
 * 
 * Run
 * docker 
 * 
 * 
 */
public class ALSStratificationChallenge {
	    
    private static final int PAGE_SIZE = 100;
    
    private static final int BATCH_SIZE = 100;
    
	private static Properties properties = null;

    private SynapseClient synapseAdmin;
    private Evaluation evaluation;
    
    public static void main( String[] args ) throws Exception {
   		ALSStratificationChallenge sct = new ALSStratificationChallenge();
   		sct.setUp();
    	sct.validate();
       	sct.score();
    }
    
    public ALSStratificationChallenge() throws SynapseException {
    	synapseAdmin = createSynapseClient();
    	String adminUserName = getProperty("ADMIN_USERNAME");
    	String adminPassword = getProperty("ADMIN_PASSWORD");
    	synapseAdmin.login(adminUserName, adminPassword);
    }
    
    public void setUp() throws SynapseException, UnsupportedEncodingException {
    	evaluation = synapseAdmin.getEvaluation(getProperty("EVALUATION_ID"));
    }
    
    /*
     * @param shellCommand
     * @param params: the params to pass to the shellCommand
     * @param workingDirectory: the working directory for the process (returned by 'cwd' in Perl)
     * @return the shell output of the command
     */
    public static String executeShellCommand(String shellCommand, String[] params, List<String> envp, File workingDirectory) throws IOException {
   	    String[] commandAndParams = new String[params.length+1];
   	    int i=0;
  	    commandAndParams[i++] = shellCommand;
   	    if (commandAndParams.length!=i+params.length) throw new IllegalStateException();
   	    System.arraycopy(params, 0, commandAndParams, i, params.length);
   	    System.out.println("\n\n");
  	    for (int j=0; j<commandAndParams.length; j++) {
   	    	System.out.print(commandAndParams[j]+" ");
   	    }
   	    System.out.println("");
   	    Process process = Runtime.getRuntime().exec(commandAndParams, envp.toArray(new String[]{}), workingDirectory);
   	    int exitValue = -1;
   	    long processStartTime = System.currentTimeMillis();
   	    long maxScriptExecutionTimeMillis = Long.parseLong(getProperty("MAX_SCRIPT_EXECUTION_TIME_MILLIS"));
   	    while (System.currentTimeMillis()-processStartTime<maxScriptExecutionTimeMillis) {
   	    	try {
   	    		exitValue = process.exitValue();
   	    		break;
   	    	} catch (IllegalThreadStateException e) {
   	    		// not done yet
   	    	}
   	    	exitValue = -1;
   	    	try {
   	    		Thread.sleep(1000L);
   	    	} catch (InterruptedException e) {
   	    		throw new RuntimeException(e);
   	    	}
   	    }
   	    if (exitValue==-1 && System.currentTimeMillis()-processStartTime>=maxScriptExecutionTimeMillis) {
   	    	throw new RuntimeException("Process exceeded alloted time.");
   	    }
   	    ByteArrayOutputStream resultOS = new ByteArrayOutputStream();
   	    String output = null;
   	    try {
   	    	if (exitValue==0) {
   	    		IOUtils.copy(process.getInputStream(), resultOS);
   	    	} else {
  	    		IOUtils.copy(process.getErrorStream(), resultOS);
   	    	}
   	    	resultOS.close();
   	    	output = new String(resultOS.toByteArray(), "UTF-8");
   	    } finally {
   	    	if (resultOS!=null) resultOS.close();
   	    }
   	    if (exitValue!=0) {
   	   	    System.out.println(output);
  	    	throw new RuntimeException(output);
   	    }
   	    System.out.println(output);
   	    return output;
    }
    
    public static  List<String> getDockerEnvParams() {
   	    List<String> envp = new ArrayList<String>();
   	    if (getProperty("DOCKER_HOST",false)!=null) envp.add("DOCKER_HOST="+getProperty("DOCKER_HOST",false));
   	    if (getProperty("DOCKER_CERT_PATH",false)!=null) envp.add("DOCKER_CERT_PATH="+getProperty("DOCKER_CERT_PATH",false));
  	    if (getProperty("DOCKER_TLS_VERIFY",false)!=null) envp.add("DOCKER_TLS_VERIFY="+getProperty("DOCKER_TLS_VERIFY",false));
  	    if (getProperty("HOME_DIR",false)!=null) envp.add("HOME="+getProperty("HOME_DIR",false));
  	    return envp;
    }
    
    public static String executeDockerCommand(String[] params, File workingDirectory) throws IOException {
   	    String dockerCommand = getProperty("DOCKER_COMMAND");
   	    List<String> envp = getDockerEnvParams();
   		return executeShellCommand(dockerCommand, params, envp, workingDirectory);
    }
    
    public static String getDockerReferenceFromFileHandle(FileHandle fileHandle) {
    	if (!(fileHandle instanceof ExternalFileHandle)) 
    		throw new RuntimeException("Submission is not a Docker reference.  Not an external file handle.");
    	ExternalFileHandle efh = (ExternalFileHandle)fileHandle;
    	String urlString = efh.getExternalURL();
    	URL url = null;
    	try {
    		url = new URL(urlString);
    		
    	} catch (MalformedURLException e) {
    		throw new RuntimeException("Submission is not a valid Docker reference (malformed URL).");
    	}
    	
    	// this should be /username/repository@sha256:hash
    	String dockerReference = url.getPath();
    	
    	// make sure it's a valid Docker reference
    	if (!isValidDockerReference(dockerReference)) {
    		throw new IllegalArgumentException("Submitted url is not of the form https://hostname/username/repository@shas256:hash");
    	}
    	
    	// need to remove the leading slash ("/")
    	dockerReference = dockerReference.substring(1);
    	
    	return dockerReference;
    }
    
    /*
     * Must be an external file handle referring to a docker image
     * must be able to retrieve the docker image
     * 
     * returns the output of 'docker pull'
     */
    public String validateSubmittedFileHandle(FileHandle fileHandle) throws IOException {
    	String dockerReference = getDockerReferenceFromFileHandle(fileHandle);
    	// 'shell out' to 'docker pull <reference>'
    	File tmpDir = Files.createTempDir();
    	String result = executeDockerCommand(new String[] {"pull", dockerReference}, tmpDir);
    	return result;
    }
    
    // we expect submissions to be URLs having 'paths' of the form:
    // /username/repository@sha256:hash
    // where username is the user's DockerHub username, aka the 'namespace',
    // repository is the name of the image
    // and hash is the 64 hex-digit sha256 hash
    public static boolean isValidDockerReference(String s) {
      	Pattern pattern = Pattern.compile("^/[a-z0-9_]{4,}/[a-zA-Z0-9-_.]+@sha256:[0-9a-f]{64}$");
		Matcher m = pattern.matcher(s);
    	return m.matches();
    }
    
    
    public void validate() throws SynapseException, IOException {
    	List<SubmissionStatus> statusesToUpdate = new ArrayList<SubmissionStatus>();
    	long total = Integer.MAX_VALUE;
       	for (int offset=0; offset<total; offset+=PAGE_SIZE) {
       			// get the newly RECEIVED Submissions
       		PaginatedResults<SubmissionBundle> submissionPGs = 
           			synapseAdmin.getAllSubmissionBundlesByStatus(evaluation.getId(), SubmissionStatusEnum.RECEIVED, offset, PAGE_SIZE);
        	total = (int)submissionPGs.getTotalNumberOfResults();
        	List<SubmissionBundle> page = submissionPGs.getResults();
        	for (int i=0; i<page.size(); i++) {
        		SubmissionBundle bundle = page.get(i);
        		Submission sub = bundle.getSubmission();
    			// here we verify that we can retrieve from DockerHub
    			FileHandle fileHandle = getFileHandleFromEntityBundle(sub.getEntityBundleJSON());
      			// Examine file to decide whether the submission is valid
       			SubmissionStatusEnum newStatus = null;
       			String validationOutput = null;
       			try {
       				validationOutput = validateSubmittedFileHandle(fileHandle);
       				newStatus = SubmissionStatusEnum.VALIDATED;
       			} catch (Exception e) {
       				newStatus = SubmissionStatusEnum.INVALID;
       				validationOutput = e.getMessage();
       				// send the user an email message to let them know
       				sendMessage(sub.getUserId(), SUB_ACK_SUBJECT, SUB_ACK_INVALID+validationOutput);
       			}
           		SubmissionStatus status = bundle.getSubmissionStatus();
           		// add validationOutput to submission status
    			addAnnotation(status, "validation-output", validationOutput, /*isPrivate*/false);
          		status.setStatus(newStatus);
           	    statusesToUpdate.add(status);
        	}
       	}
       	// we can update all the statuses in a batch
       	updateSubmissionStatusBatch(statusesToUpdate);
    }
    
    private static final String SUB_ACK_SUBJECT = "Submission Acknowledgment";
    private static final String SUB_ACK_INVALID = "Your submission is invalid. Please try again.  Reason:\n\n";
       
    private void sendMessage(String userId, String subject, String body) throws SynapseException {
    	MessageToUser messageMetadata = new MessageToUser();
    	messageMetadata.setRecipients(Collections.singleton(userId));
    	messageMetadata.setSubject(subject);
    	synapseAdmin.sendStringMessage(messageMetadata, body);
    }
    
    public static String[] dockerParams(File ioDirectory, String dockerReference) {
    	String[] result = new String[5];
    	int i = 0;
    	result[i++] = "run";
    	result[i++] = "-v";
    	result[i++] = ioDirectory.getAbsolutePath()+":/model_folder";
    	result[i++] = dockerReference;
      	result[i++] = "/run.sh";
    	return result;
    }
    
    public static File modelInputFile(File ioDirectory) {
    	return new File(ioDirectory, "in.txt");
    }
    
    public static File modelOutputFile(File ioDirectory) {
    	return new File(ioDirectory, "out.txt");
    }
    
    public String scoreOneModel(FileHandle fileHandle, File inputFile) throws IOException {
    	String dockerReference = getDockerReferenceFromFileHandle(fileHandle);
    	File workingDir = null;
    	Boolean usingBoot2Docker = Boolean.valueOf(getProperty("USING_BOOT2_DOCKER"));
    	if (usingBoot2Docker) {
    		// if using boot2Docker make the temporary folder in our home directory
    		workingDir = new File(getProperty("HOME_DIR"), UUID.randomUUID().toString());
    		workingDir.mkdir();
    		workingDir.deleteOnExit();
    	} else {
        	workingDir = Files.createTempDir();
    	}
    	Files.copy(inputFile, new File(workingDir, inputFile.getName()));
    	String[] params = dockerParams(workingDir, dockerReference);
    	// we could also capture the shell output as an annotation or to send to the submitter by email
    	executeDockerCommand(params, workingDir);
    	InputStream is = new FileInputStream(modelOutputFile(workingDir));
    	try {
    		String outputFileContent = IOUtils.toString(is);
    		return outputFileContent;
    	} finally {
    		is.close();
    	}
    }
    
    // this downloads a Synapse File using its original file name
    private File downloadInputFile() throws IOException, SynapseException, JSONObjectAdapterException {
		// read input file
		String synId = getProperty("SCORING_DATA_ID");
		File tmpDir = Files.createTempDir();
		List<FileHandle> fhs = synapseAdmin.getEntityFileHandlesForCurrentVersion(synId).getList();
		FileHandle fileHandleToDownload = null;
		for (FileHandle fh : fhs) {
			if (!(fh instanceof PreviewFileHandle)) {
				fileHandleToDownload = fh;
				break;
			}
		}
		if (fileHandleToDownload==null) throw new RuntimeException("No file handle for "+synId);
		File destinationFile = new File(tmpDir, fileHandleToDownload.getFileName());
		synapseAdmin.downloadFromFileEntityCurrentVersion(synId, destinationFile);
		return destinationFile;
    }
    
    /**
     * Note: There are two types of scoring, that in which each submission is scored alone and that
     * in which the entire set of submissions is rescored whenever a new one arrives. 
     * 
     * @throws SynapseException
     * @throws JSONObjectAdapterException 
     */
    public void score() throws SynapseException, IOException, JSONObjectAdapterException {
    	long startTime = System.currentTimeMillis();
    	List<SubmissionStatus> statusesToUpdate = new ArrayList<SubmissionStatus>();
    	long total = Integer.MAX_VALUE;
    	File inputFile = null;
       	for (int offset=0; offset<total; offset+=PAGE_SIZE) {
       		PaginatedResults<SubmissionBundle> submissionPGs = null;
       		// just get the unscored submissions in the Evaluation
       		// here we get the ones that the 'validation' step (above) marked as validated
       		submissionPGs = synapseAdmin.getAllSubmissionBundlesByStatus(evaluation.getId(), SubmissionStatusEnum.VALIDATED, offset, PAGE_SIZE);
        	total = (int)submissionPGs.getTotalNumberOfResults();
        	List<SubmissionBundle> page = submissionPGs.getResults();
        	for (int i=0; i<page.size(); i++) {
        		SubmissionBundle bundle = page.get(i);
        		Submission sub = bundle.getSubmission();
        		FileHandle fileHandle = getFileHandleFromEntityBundle(sub.getEntityBundleJSON());
        		if (inputFile==null) {
        			String synId = getProperty("SCORING_DATA_ID");
        			String inputFileName = getProperty("INPUT_FILE_NAME");
        			File tmpDir = Files.createTempDir();
        			inputFile = new File(tmpDir, inputFileName);
        			synapseAdmin.downloadFromFileEntityCurrentVersion(synId, inputFile);
        		}
           		SubmissionStatus status = bundle.getSubmissionStatus();
           		try {
           	        String result = scoreOneModel(fileHandle, inputFile);
           	        // push result as score (just for demo purposed)
           	        addAnnotation(status, "model-score", result, /*isPrivate*/false);
           	        removeAnnotation(status, "scoring-failure");
           			status.setStatus(SubmissionStatusEnum.SCORED);
           		} catch (Exception e) {
           			addAnnotation(status, "scoring-failure", e.getMessage(), /*isPrivate*/false);
           	        removeAnnotation(status, "model-score");
          			status.setStatus(SubmissionStatusEnum.REJECTED);
           			// here we could send failure notification to submitter
           			e.printStackTrace();
           		}
     			statusesToUpdate.add(status);
        	}
       	}
       	
       	System.out.println("Retrieved "+total+" submissions for scoring.");
       	
       	updateSubmissionStatusBatch(statusesToUpdate);
       	
       	System.out.println("Scored "+statusesToUpdate.size()+" submissions.");
       	long delta = System.currentTimeMillis() - startTime;
       	System.out.println("Elapsed time for running scoring app: "+formatInterval(delta));
    }
    
    private static final int BATCH_UPLOAD_RETRY_COUNT = 3;
    
    private void updateSubmissionStatusBatch(List<SubmissionStatus> statusesToUpdate) throws SynapseException {
       	// now we have a batch of statuses to update
    	for (int retry=0; retry<BATCH_UPLOAD_RETRY_COUNT; retry++) {
    		try {
		       	String batchToken = null;
		       	for (int offset=0; offset<statusesToUpdate.size(); offset+=BATCH_SIZE) {
		       		SubmissionStatusBatch updateBatch = new SubmissionStatusBatch();
		       		List<SubmissionStatus> batch = new ArrayList<SubmissionStatus>();
		       		for (int i=0; i<BATCH_SIZE && offset+i<statusesToUpdate.size(); i++) {
		       			batch.add(statusesToUpdate.get(offset+i));
		       		}
		       		updateBatch.setStatuses(batch);
		       		boolean isFirstBatch = (offset==0);
		       		updateBatch.setIsFirstBatch(isFirstBatch);
		       		boolean isLastBatch = (offset+BATCH_SIZE)>=statusesToUpdate.size();
		       		updateBatch.setIsLastBatch(isLastBatch);
		       		updateBatch.setBatchToken(batchToken);
		       		BatchUploadResponse response = 
		       				synapseAdmin.updateSubmissionStatusBatch(evaluation.getId(), updateBatch);
		       		batchToken = response.getNextUploadToken();
		       	}
		       	break; // success!
    		} catch (SynapseConflictingUpdateException e) {
    			// we collided with someone else access the Evaluation.  Will retry!
    		}
    		throw new RuntimeException("Failed to update submission status(es) after "+BATCH_UPLOAD_RETRY_COUNT+" tries.");
    	}
    }
    
    private static FileHandle getFileHandleFromEntityBundle(String s) {
    	try {
	    	JSONObject bundle = new JSONObject(s);
	    	JSONArray fileHandles = (JSONArray)bundle.get("fileHandles");
	    	for (int i=0; i<fileHandles.length(); i++) {
	    		String jsonString = fileHandles.getString(i);
    			FileHandle fileHandle = EntityFactory.createEntityFromJSONString(jsonString, FileHandle.class);
    			if (!(fileHandle instanceof PreviewFileHandle)) return fileHandle;
	    	}
	    	throw new IllegalArgumentException("File has no file handle ID");
    	} catch (JSONException e) {
    		throw new RuntimeException(e);
    	} catch (JSONObjectAdapterException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    private static String formatInterval(final long l) {
        final long hr = TimeUnit.MILLISECONDS.toHours(l);
        final long min = TimeUnit.MILLISECONDS.toMinutes(l - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02dh:%02dm:%02d.%03ds", hr, min, sec, ms);
    }
    
     
	// the 'isPrivate' flag should be set to 'true' for information
	// used by the scoring application but not to be revealed to participants
	// to see 'public' annotations requires READ access in the Evaluation's
	// access control list, as the participant has (see setUp(), above). To
	// see 'private' annotatations requires READ_PRIVATE_SUBMISSION access,
	// which the Evaluation admin has by default
    private static void addAnnotation(SubmissionStatus status, String key, String value, boolean isPrivate) {
    	if (value.length()>499) value = value.substring(0, 499);
    	Annotations annotations = status.getAnnotations();
		if (annotations==null) {
			annotations=new Annotations();
			status.setAnnotations(annotations);
		}
		List<StringAnnotation> sas = annotations.getStringAnnos();
		if (sas==null) {
			sas = new ArrayList<StringAnnotation>();
			annotations.setStringAnnos(sas);
		}
		StringAnnotation matchingSa = null;
		for (StringAnnotation existingSa : sas) {
			if (existingSa.getKey().equals(key)) {
				matchingSa = existingSa;
				break;
			}
		}
		if (matchingSa==null) {
			StringAnnotation sa = new StringAnnotation();
			sa.setIsPrivate(isPrivate);
			sa.setKey(key);
			sa.setValue(value);
			sas.add(sa);
		} else {
			matchingSa.setIsPrivate(isPrivate);
			matchingSa.setValue(value);
		}
    }
        
    private static void removeAnnotation(SubmissionStatus status, String key) {
    	Annotations annotations = status.getAnnotations();
		if (annotations==null) return;
		List<StringAnnotation> sas = annotations.getStringAnnos();
		if (sas==null) return;
		for (StringAnnotation existingSa : sas) {
			if (existingSa.getKey().equals(key)) {
				sas.remove(existingSa);
			}
		}
    }
        
	public static void initProperties() {
		if (properties!=null) return;
		properties = new Properties();
		InputStream is = null;
    	try {
    		is = ALSStratificationChallenge.class.getClassLoader().getResourceAsStream("global.properties");
    		properties.load(is);
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	} finally {
    		if (is!=null) try {
    			is.close();
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	}
   }
	
	public static String getProperty(String key) {
		return getProperty(key, true);
	}
		
	public static String getProperty(String key, boolean required) {
		initProperties();
		String commandlineOption = System.getProperty(key);
		if (commandlineOption!=null) return commandlineOption;
		String embeddedProperty = properties.getProperty(key);
		if (embeddedProperty!=null) return embeddedProperty;
		if (required) throw new RuntimeException("Cannot find value for "+key);
		return null;
	}	
	  
	private static SynapseClient createSynapseClient() {
		SynapseClientImpl scIntern = new SynapseClientImpl();
		scIntern.setAuthEndpoint("https://repo-prod.prod.sagebase.org/auth/v1");
		scIntern.setRepositoryEndpoint("https://repo-prod.prod.sagebase.org/repo/v1");
		scIntern.setFileEndpoint("https://repo-prod.prod.sagebase.org/file/v1");
		return SynapseProfileProxy.createProfileProxy(scIntern);
  }

}
