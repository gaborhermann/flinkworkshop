package hu.sztaki.workshop.flink.day9.solutions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.example.utils.ExampleUtils;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

public class MaximumWeightPropagation {

	/**
	 * MaximumWeight
	 *
	 * Create a vertex-weighted graph. Set the weight of every vertex to the maximal Vertex weight.
	 * Use vertex-centric iteration.
	 *
	 * Hint:
	 * Every superstep:
	 * Send every neighbour the vertex weight.
	 * Get the maximum of received weights and update weights accordingly.
	 *
	 * Tamper with the maximum iteration number and setNewVertexValue at every update. See what happens.
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		Graph<Long, Double, Double> graph = Graph.fromDataSet(ExampleUtils.getLongDoubleVertexData(env), ExampleUtils.getLongDoubleEdgeData(env), env);

		graph.runVertexCentricIteration(
				VertexCentricIteration.withEdges(
						graph.getEdges(),
						new VertexUpdateFunction<Long, Double, Double>() {
							@Override
							public void updateVertex(Long vertexId, Double weight, MessageIterator<Double> messageIterator) throws Exception {
								double maxWeight = 0;
								for (Double gotNeighbourWeights : messageIterator) {
									if (maxWeight < gotNeighbourWeights) {
										maxWeight = gotNeighbourWeights;
									}
								}

								// TODO omit if statement, see what happens
								if (weight < maxWeight) {
									setNewVertexValue(maxWeight);
								}
							}
						},
						new MessagingFunction<Long, Double, Double, Double>() {
							@Override
							public void sendMessages(Long vertexId, Double weight) throws Exception {
								sendMessageToAllNeighbors(weight);
							}
						}, 20)
		).getVertices().print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}