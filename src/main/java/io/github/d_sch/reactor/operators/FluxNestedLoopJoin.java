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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodePredicate;
import lombok.val;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class FluxNestedLoopJoin {

	public static Flux<ObjectNode> fromIterableChecked(List<ObjectNode> monoList) {
		return Flux.defer(
				() -> Flux.fromIterable(
						monoList
				).transform(
						flux -> checked(
								flux
						)
				)
		);
	}

	public static Flux<ObjectNode> checked(Flux<ObjectNode> objectNodes) {
		return objectNodes.<ObjectNode>handle(
				(jsonNodes, sink) -> {
					if (!jsonNodes.isEmpty()) {
						sink.next(
								jsonNodes
						);
					} else {
						sink.error(
								new IllegalArgumentException(
										"Empty objects are not allowed!"
								)
						);
					}
				}
		);
	}

	public static Flux<ObjectNode> innerJoin(NodePredicate predicate, Flux<ObjectNode> left,
			Mono<List<ObjectNode>> right) {
		return nestedLoopJoin(
				NestedLoopJoinType.INNER_JOIN, predicate, left, right
		);
	}

	public static Flux<ObjectNode> leftOuterJoin(NodePredicate predicate, Flux<ObjectNode> left,
			Mono<List<ObjectNode>> right) {
		return nestedLoopJoin(
				NestedLoopJoinType.LEFT_OUTER_JOIN, predicate, left, right
		);
	}

	public static Flux<ObjectNode> nestedLoopJoin(NestedLoopJoinType joinType, NodePredicate predicate,
			Flux<ObjectNode> left, Mono<List<ObjectNode>> right) {
		var om = new ObjectMapper();

		Flux<ObjectNode> rightChecked = right.flatMapMany(
				list -> fromIterableChecked(
						list
				)
		);
		Flux<ObjectNode> leftChecked = left.transform(
				flux -> checked(
						flux
				)
		);

		return Mono.just(
				Tuples.of(
						leftChecked, rightChecked
				)
		).flatMapMany(
				objects -> objects.getT1().concatMap(
						leftObject -> objects.getT2().filter(
								rightObject -> predicate.test(
										leftObject, rightObject
								)
						).map(
								rightObject -> {
									var jsonObject = om.createObjectNode();
									jsonObject.setAll(
											leftObject
									);
									jsonObject.setAll(
											rightObject
									);
									return jsonObject;
								}
						).transform(
								flux -> {
									if (joinType == NestedLoopJoinType.INNER_JOIN) {
										return flux;
									} else {
										return flux.switchIfEmpty(
												Mono.fromSupplier(
														() -> {
															var jsonObject = om.createObjectNode();
															jsonObject.setAll(
																	leftObject
															);
															return jsonObject;
														}
												)
										);
									}
								}
						)
				)
		);
	}
}
