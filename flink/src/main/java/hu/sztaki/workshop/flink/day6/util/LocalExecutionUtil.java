package hu.sztaki.workshop.flink.day6.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.util.ClusterUtil;

public class LocalExecutionUtil {

	/**
	 * Executes the given JobGraph locally, on a FlinkMiniCluster
	 *
	 * @param jobGraph
	 *            jobGraph
	 * @param parallelism
	 *            numberOfTaskTrackers
	 * @param memorySize
	 *            memorySize
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	public static JobExecutionResult runOnMiniCluster(JobGraph jobGraph, int parallelism, long memorySize) throws Exception {
		return ClusterUtil.runOnMiniCluster(jobGraph, parallelism, memorySize);
	}
}
