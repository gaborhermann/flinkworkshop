package hu.sztaki.workshop.flink.day9;

import org.apache.flink.api.java.ExecutionEnvironment;

public class MaximumWeightPropagation {

	/**
	 * MaximumWeightPropagation
	 *
	 * Create a vertex-weighted graph. Set the weight of every vertex to the maximal Vertex weight.
	 * Use vertex-centric iteration.
	 *
	 * Every superstep:
	 * Send every neighbour the vertex weight.
	 * Get the maximum of received weights and update weights accordingly.
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}