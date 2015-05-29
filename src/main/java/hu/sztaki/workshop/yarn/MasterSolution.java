package hu.sztaki.workshop.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {
    private static Logger logger = LoggerFactory.getLogger(Master.class);

    /**
     * @todo Configuration, RM, NM and callback handlers.
     */
    private Configuration configuration = new YarnConfiguration();

    private AMRMClientAsync resourceManager;
    private NMClientAsync nodeManager;
    private NMCallbackHandler containerListener;

    private ApplicationAttemptId attemptID;

    /**
     * @todo Track container statuses.
     */
    private AtomicInteger nCompletedContainers  = new AtomicInteger();
    private AtomicInteger nAllocatedContainers  = new AtomicInteger();
    private AtomicInteger nFailedContainers     = new AtomicInteger();
    private AtomicInteger nRequestedContainers  = new AtomicInteger();
    private int nTotalContainers                = 2;

    private Map<String, String> shellEnvironment = new HashMap<>();
    private List<Thread> launchedThreads = new ArrayList<>();

    private int masterRPCPort = 0;
    private String masterTrackingURL = "";
    private String masterHostName= "";
    private int containerMemory = 512;

    private volatile boolean done;
    private volatile boolean success;

    public static void main(String[] args){
        logger.info("ApplicationMaster started");
        try{
            Master masterThread = new Master();
            if(masterThread.run(args)){
                logger.info("ApplicationMaster completed successfully");
                System.exit(0);
            }else{
                logger.info("ApplicationMaster failed");
                System.exit(2);
            }
        }catch(Throwable t){
            logger.error("Error running ApplicationMaster");
            t.printStackTrace();
            System.exit(1);
        }
    }

    public boolean run(String[] args) throws YarnException, IOException{
        AMRMClientAsync.CallbackHandler allocationListener = new RMCallbackHandler();

        /**
         * @todo Instantiate ResourceManager handler that will manage events such as container allocations and completions.
         */
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocationListener);
        resourceManager.init(configuration);
        resourceManager.start();

        logger.info("Currently " + resourceManager.getClusterNodeCount() + " nodes on the cluster");

        /**
         * @todo Instantiate NodeManager event handler.
         */
        containerListener = new NMCallbackHandler();

        /**
         * @todo Encapsulate the NodeManager client.
         */
        nodeManager = new NMClientAsyncImpl(containerListener);
        nodeManager.init(configuration);
        nodeManager.start();

        /**
         * @todo Register the ApplicationMaster with the ResourceManager.
         */
        RegisterApplicationMasterResponse response =
                resourceManager.registerApplicationMaster(masterHostName, masterRPCPort, masterTrackingURL);

        /**
         * @todo Update container memory from RM response.
         */
        int maximumMemory = response.getMaximumResourceCapability().getMemory();
        if(containerMemory > maximumMemory){
            containerMemory = maximumMemory;
        }

        logger.info("Using " + containerMemory + " MB of container memory");

        /**
         * @todo Request containers to number of total containers and do this until all has been allocated and run.
         */
        for(int i = 0; i < nTotalContainers; ++i){
            AMRMClient.ContainerRequest containerRequest = setupContainerAskForRM();
            resourceManager.addContainerRequest(containerRequest);
        }
        nRequestedContainers.set(nTotalContainers);

        while(!done){
            try{
                Thread.sleep(200);
            }catch(InterruptedException e){
                logger.warn("ApplicationMaster interrupted");
            }
        }

        logger.info("ApplicationMaster done");
        logger.info("Finishing and cleaning");

        /**
         * @todo Finish and clean the ApplicationMaster.
         */
        finish();

        return success;
    }

    private void finish() throws IOException, YarnException{
        /**
         * @todo Join all threads.
         */
        for(Thread thread : launchedThreads){
            try{
                thread.join();
            }catch (InterruptedException e){
                logger.warn("Interrupted exception while finishing");
            }
        }

        /**
         * @todo Unregister the ApplicationMaster from the ResourceManager.
         * @todo Set final application status based on completed containers.
         */
        FinalApplicationStatus applicationStatus;
        if(nFailedContainers.get() == 0 && nCompletedContainers.get() == nTotalContainers){
            applicationStatus = FinalApplicationStatus.SUCCEEDED;
        }else{
            applicationStatus = FinalApplicationStatus.FAILED;
        }

        resourceManager.unregisterApplicationMaster(applicationStatus, null, null);

        done = true;

        resourceManager.stop();
        nodeManager.stop();
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler{
        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            /**
             * @todo Iterate over statuses and update our counters for container statuses.
             */
            for(ContainerStatus containerStatus : statuses){
                assert(containerStatus.getState() == ContainerState.COMPLETE);
                int exitStatus = containerStatus.getExitStatus();
                if(0 != exitStatus){
                    if(ContainerExitStatus.ABORTED != exitStatus){
                        nCompletedContainers.incrementAndGet();
                        nFailedContainers.incrementAndGet();
                    }else{
                        nAllocatedContainers.decrementAndGet();
                        nRequestedContainers.decrementAndGet();
                    }
                }else{
                    nCompletedContainers.incrementAndGet();
                }
            }

            /**
             * @todo Ask for additional containers if not reached total.
             * @see org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
             */
            int askCount = nTotalContainers - nRequestedContainers.get();
            nRequestedContainers.addAndGet(askCount);

            logger.info("Asking for " + askCount + " containers");

            if(askCount > 0){
                for(int i = 0; i < askCount; ++i){
                    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
                    resourceManager.addContainerRequest(containerAsk);
                }
            }
            if(nCompletedContainers.get() == nTotalContainers){
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> containers) {
            /**
             * @todo Update number of allocated containers.
             * @todo Foreach container actually run it.
             */
            nAllocatedContainers.addAndGet(containers.size());

            for(Container container : containers){
                logger.info("Handling container allocation for container" + container.getId());
                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(container, containerListener);
                Thread thread = new Thread(runnableLaunchContainer);
                launchedThreads.add(thread);
                thread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            /**
             * @todo Terminated.
             */
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void onError(Throwable e) {
            /**
             * @todo Terminated. Stop resource manager callback handler.
             */
            done = true;
            resourceManager.stop();
        }
    }

    private class LaunchContainerRunnable implements Runnable {
        Container container;
        NMCallbackHandler callbackHandler;

        public LaunchContainerRunnable(Container container, NMCallbackHandler callbackHandler) {
            this.container = container;
            this.callbackHandler = callbackHandler;
        }

        @Override
        public void run() {
            /**
             * @todo Construct and setup a container launch context.
             */
            logger.info("Container launcher is running for container " + container.getId());
            ContainerLaunchContext containerLaunchContext =
                    Records.newRecord(ContainerLaunchContext.class);

            List<String> commands = new ArrayList<String>();

            commands.add("java -version 2> <LOG_DIR>/stdout");

            containerLaunchContext.setEnvironment(shellEnvironment);
            containerLaunchContext.setCommands(commands);

            /**
             * @todo Start the container using NodeManager.
             */

            containerListener.addContainer(container.getId(), container);
            nodeManager.startContainerAsync(container, containerLaunchContext);


            logger.info("Container launcher initialized and added " + container.getId());

            success = true;
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler{
        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerID, Container container){
            containers.putIfAbsent(containerID, container);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            logger.info("Container started");
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            logger.info("Container status received");
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            logger.info("Container stopped");
            containers.remove(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            logger.info("Failed to start container " + containerId);
            containers.remove(containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            logger.info("Container status error received");
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            logger.info("On stop container error received");
            containers.remove(containerId);
        }
    }

    private AMRMClient.ContainerRequest setupContainerAskForRM(){
        logger.info("Creating new container request");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(1);

        return new AMRMClient.ContainerRequest(capability, null, null, Priority.newInstance(1));
    }
}
