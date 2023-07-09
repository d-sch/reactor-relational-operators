/*
 * Copyright (c) 2023 d-sch <https://github.com/d-sch>
 * Copyright 2023 d-sch <https://github.com/d-sch>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */
package io.github.d_sch.reactor.operators;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodeHash;
import io.github.d_sch.reactor.common.NodePredicate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class FluxHashMatchJoin {

	static private ObjectMapper om = new ObjectMapper();

	public static <K> Flux<ObjectNode> innerJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.INNER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> innerJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.INNER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> leftOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Mono<List<ObjectNode>> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.LEFT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> leftOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.LEFT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> rightOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Mono<List<ObjectNode>> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.RIGHT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> rightOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.RIGHT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> fullOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Mono<List<ObjectNode>> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.FULL_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> fullOuterJoin(NodeHash<K> hashLeft, NodeHash<K> hashRight,
			NodePredicate predicate, Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				HashMatchJoinType.FULL_OUTER_JOIN, hashLeft, hashRight, predicate, left, right
		);
	}

	public static <K> Flux<ObjectNode> hashMatchJoin(HashMatchJoinType joinType, NodeHash<K> hashLeft,
			NodeHash<K> hashRight, NodePredicate predicate, Mono<List<ObjectNode>> left, Flux<ObjectNode> right) {
		return hashMatchJoin(
				joinType, hashLeft, hashRight, predicate, left.flatMapMany(
						list -> Flux.fromIterable(
								list
						)
				), right
		);
	}

	public static <K> Flux<ObjectNode> hashMatchJoin(HashMatchJoinType joinType, NodeHash<K> hashLeft,
			NodeHash<K> hashRight, NodePredicate predicate, Flux<ObjectNode> left, Flux<ObjectNode> right) {

		var leftMap = buildLeftMap(
				hashLeft, left
		);

		return right.transform(
				innerFlux -> probing(
						joinType, innerFlux, hashRight, leftMap, predicate
				)
		).filter(
				Predicate.not(
						ObjectNode::isEmpty
				)
		).transform(
				flux -> handleLeftUnmatched(
						joinType, om, leftMap, flux
				)
		);
	}

	private static <K> Flux<ObjectNode> probing(HashMatchJoinType joinType, Flux<ObjectNode> innerFlux,
			NodeHash<K> hashRight, Mono<Map<K, Tuple2<ObjectNode, Boolean>>> leftMap, NodePredicate predicate) {
		if (joinType == HashMatchJoinType.RIGHT_OUTER_JOIN || joinType == HashMatchJoinType.FULL_OUTER_JOIN) {
			return innerFlux.withLatestFrom(
					leftMap, (rightObject, map) -> {
						var merged = om.createObjectNode();
						var result = probeRight(
								hashRight, predicate, rightObject, map
						);
						if (result != null && result.getT2()) {
							merged.setAll(
									result.getT1()
							);
							merged.setAll(
									rightObject
							);
						} else {
							merged.setAll(
									rightObject
							);
						}
						return merged;
					}
			);
		} else {
			return innerFlux.withLatestFrom(
					leftMap, (rightObject, map) -> {
						var merged = om.createObjectNode();
						var result = probeRight(
								hashRight, predicate, rightObject, map
						);
						if (result != null && result.getT2()) {
							merged.setAll(
									result.getT1()
							);
							merged.setAll(
									rightObject
							);
						}
						return merged;
					}
			);
		}
	}

	private static <K> Flux<ObjectNode> handleLeftUnmatched(HashMatchJoinType joinType, ObjectMapper om,
			Mono<Map<K, Tuple2<ObjectNode, Boolean>>> leftMap, Flux<ObjectNode> flux) {
		if (joinType == HashMatchJoinType.LEFT_OUTER_JOIN || joinType == HashMatchJoinType.FULL_OUTER_JOIN) {
			return flux.concatWith(
					leftMap.flatMapMany(
							map -> Flux.fromIterable(
									map.values()
							)
					).filter(
							Predicate.not(
									Tuple2::getT2
							)
					).map(
							tuple -> {
								var merged = om.createObjectNode();
								merged.setAll(
										tuple.getT1()
								);
								return merged;
							}
					)
			);
		} else {
			return flux;
		}
	}

	private static <K> Tuple2<ObjectNode, Boolean> probeRight(NodeHash<K> hashRight, NodePredicate predicate,
			ObjectNode rightObject, Map<K, Tuple2<ObjectNode, Boolean>> map) {
		return map.computeIfPresent(
				hashRight.apply(
						rightObject
				), (k, tuple) -> {
					if (predicate.test(
							tuple.getT1(), rightObject
					)) {
						return tuple.mapT2(
								old -> true
						);
					}
					return tuple;
				}
		);
	}

	private static <K> Mono<Map<K, Tuple2<ObjectNode, Boolean>>> buildLeftMap(NodeHash<K> hashLeft,
			Flux<ObjectNode> left) {
		return left.map(
				jsonNodes -> Tuples.of(
						hashLeft.apply(
								jsonNodes
						), Tuples.of(
								jsonNodes, false
						)
				)
		).collectMap(
				Tuple2::getT1, Tuple2::getT2
		);
	}

}
