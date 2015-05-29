package hu.sztaki.workshop.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Class implements the application submission logic.
 */
public class Application implements Tool{
    /**
     * Just a logging mechanism.
     */
    Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * YARN configuration.
     */
    Configuration configuration = new YarnConfiguration();
    /**
     * YARN client can be used to setup and submit applications.
     */
    YarnClient client;

    /**
     * Amount of memory we would like to request from the ResourceManager.
     */
    private int requestedMemory = 512;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Application(), args);
    }

    public int run(String[] args) throws Exception {
        /**
         * @todo Create, initialize and start YARN client.
         */
        client = YarnClient.createYarnClient();
        client.init(getConf());
        client.start();

        /**
         * @todo Create an application using the client object.
         * @todo Create response and submission context using the application.
         */
        YarnClientApplication application = client.createApplication();
        GetNewApplicationResponse applicationResponse = application.getNewApplicationResponse();
        ApplicationSubmissionContext submissionContext = application.getApplicationSubmissionContext();

        /**
         * @todo Log the cluster resources and adjust requested memory if necessary.
         */
        logger.info("Maximum memory to allocate: " + applicationResponse.getMaximumResourceCapability().getMemory());
        logger.info("Maximum cores to allocate: " + applicationResponse.getMaximumResourceCapability().getVirtualCores());

        int maxMemory = applicationResponse.getMaximumResourceCapability().getMemory();
        if(maxMemory < requestedMemory){
            requestedMemory = maxMemory;
        }

        /**
         * @todo Get a new ID of our application using the submission context.
         */
        ApplicationId applicationID = submissionContext.getApplicationId();

        /**
         * @todo Set additional attributes, like application name, attempts, priority and queue.
         */
        submissionContext.setApplicationName("My custom YARN framework");

        logger.info("Received application ID: " + applicationID);

        submissionContext.setMaxAppAttempts(1);
        submissionContext.setPriority(Priority.newInstance(32));
        submissionContext.setQueue("default");

        /**
         * @todo Copy our application artifact (JAR) to HDFS.
         * @see Path
         * @see FileSystem
         */
        Path source = new Path("/home/ehnalis/Projects/yarn-applications/out/" +
                               "artifacts/yarn_applications_jar/yarn-applications.jar");

        FileSystem fileSystem = FileSystem.get(client.getConfig());

        Path destination = new Path(fileSystem.getHomeDirectory(),
                                    submissionContext.getApplicationName() + File.separator +
                                    applicationID.getId() + File.separator + "yarn-application.jar");

        String applicationURI = destination.toUri().toString();

        logger.info("Coping application JAR to HDFS");

        fileSystem.copyFromLocalFile(false, true, source, destination);

        /**
         * @todo Check if copy was successful by getting the file status.
         * @see FileSystem
         */
        FileStatus destinationStatus = fileSystem.getFileStatus(destination);

        if(destinationStatus.isFile()){
            logger.info("Application successfully copied with block size " + destinationStatus.getBlockSize());
        }else{
            logger.error("Application copy was unsuccessful!");
            throw new RuntimeException();
        }

        /**
         * @todo Create a local resource from the copied JAR file using Records class.
         * @todo Set type, visibility, resource, timestamp and size for the local resource.
         */
        LocalResource applicationMasterJAR = Records.newRecord(LocalResource.class);
        applicationMasterJAR.setType(LocalResourceType.FILE);
        applicationMasterJAR.setVisibility(LocalResourceVisibility.APPLICATION);
        applicationMasterJAR.setResource(ConverterUtils.getYarnUrlFromPath(destination));
        applicationMasterJAR.setTimestamp(destinationStatus.getModificationTime());
        applicationMasterJAR.setSize(destinationStatus.getLen());

        /**
         * @todo Create a container launch context using the Records class.
         */
        ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);

        /**
         * @todo Create a Map<String, LocalResource> of local resources and put our JAR resource into.
         */
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        localResources.put("yarn-application.jar", applicationMasterJAR);

        /**
         * @todo Give the local resources Map to the container launch context.
         */
        launchContext.setLocalResources(localResources);

        logger.info("ApplicationMaster's JAR registered as resource");

        /**
         * @todo Create an environment that is a map of String to String.
         */
        Map<String, String> environment = new HashMap<>();

        /**
         * @todo Use the current environment's classpath and add it to our environment.
         */
        StringBuilder classPath = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
                .append(File.pathSeparatorChar)
                .append("./*");

        logger.info("Environment set as: " + classPath);

        for(String cp : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                                 YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)){
            classPath.append(File.pathSeparator);
            classPath.append(cp.trim());
        }

        environment.put("CLASSPATH", classPath.toString());

        launchContext.setEnvironment(environment);

        logger.info("Environment variables set");

        /**
         * @todo Create a Vector of character sequences and build up our command that starts the ApplicationMaster.
         * @todo Then, add the commands to the container launch context.
         */
        Vector<CharSequence> vargs = new Vector<>(30);

        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + requestedMemory + "m");
        vargs.add("hu.sztaki.workshop.yarn.Master");
        vargs.add("-Djava.io.tmpdir={{PWD}}/tmp");
        vargs.add("1> <LOG_DIR>/stdout");
        vargs.add("2> <LOG_DIR>/stdout");
        vargs.add("--jar " + String.valueOf(applicationURI));

        StringBuilder command = new StringBuilder();
        for(CharSequence string : vargs){
            command.append(string).append(" ");
        }

        List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        launchContext.setCommands(commands);

        /**
         * @todo Now, that the AM container specifications are complete, add it to the submission context.
         */
        submissionContext.setAMContainerSpec(launchContext);

        /**
         * @todo Set a resource record for memory and add it to the submission context.
         */
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(requestedMemory);

        submissionContext.setResource(capability);

        submissionContext.setPriority(Priority.newInstance(1));

        /**
         * @todo Use the client to submit the application.
         */
        client.submitApplication(submissionContext);

        /**
         * @todo Create a monitoring method.
         */
        return (monitorApplication(applicationID) ? 1 : 0);
    }

    /**
     * Monitors application state.
     * @param applicationID Application to monitor.
     * @return Status of the application.
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId applicationID) throws YarnException, IOException{
        /**
         * @todo Monitor in every second.
         */
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }

            /**
             * @todo Use the client to fetch an application report.
             */
            ApplicationReport report = client.getApplicationReport(applicationID);

            logger.info("Fetching application report for application ID: " + applicationID);

            /**
             * @todo Read the application state and final status from the report.
             */
            YarnApplicationState applicationState = report.getYarnApplicationState();
            FinalApplicationStatus finalApplicationStatus = report.getFinalApplicationStatus();

            /**
             * @todo Log information received and return.
             */
            if(YarnApplicationState.FINISHED == applicationState){
                if(FinalApplicationStatus.SUCCEEDED == finalApplicationStatus){
                    logger.info("Application has completed successfully");
                    return true;
                }else{
                    logger.info("Application did not succeed");
                    return false;
                }
            }else{
                logger.info("Application did not finished");
                return false;
            }
        }
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }
}
