/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hu.sztaki.workshop.flink.day9.util;

import java.util.Arrays;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class GraphGenerator {

	public static Graph<Long, NullValue, NullValue> getFixedSmallGraph(ExecutionEnvironment env) {
		return Graph.fromCollection(Arrays.asList(
				new Edge<Long, NullValue>(1L, 2L, NullValue.getInstance()),
				new Edge<Long, NullValue>(1L, 4L, NullValue.getInstance()),
				new Edge<Long, NullValue>(2L, 4L, NullValue.getInstance()),
				new Edge<Long, NullValue>(3L, 2L, NullValue.getInstance()),
				new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()),
				new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()),
				new Edge<Long, NullValue>(5L, 6L, NullValue.getInstance()),
				new Edge<Long, NullValue>(6L, 2L, NullValue.getInstance())
		), env);
	}

	public static Graph<Long, Double, Double> getFixedSmallWeightedGraph(ExecutionEnvironment env) {
		return null;
	}
}
