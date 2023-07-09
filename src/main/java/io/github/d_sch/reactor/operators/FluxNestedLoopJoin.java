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
                ).transform(flux -> checked(flux))
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

    public static Mono<List<ObjectNode>> innerJoin(NodePredicate predicate, Flux<ObjectNode> left,
            Mono<List<ObjectNode>> right) {
        return nestedLoopJoin(
                NestedLoopJoinType.INNER_JOIN, predicate, left, right
        ).collectList();
    }

    public static Mono<List<ObjectNode>> leftOuterJoin(NodePredicate predicate, Flux<ObjectNode> left,
            Mono<List<ObjectNode>> right) {
        return nestedLoopJoin(
                NestedLoopJoinType.LEFT_OUTER_JOIN, predicate, left, right
        ).collectList();
    }

    public static Flux<ObjectNode> nestedLoopJoin(NestedLoopJoinType joinType, NodePredicate predicate,
            Flux<ObjectNode> left, Mono<List<ObjectNode>> right) {
        var om = new ObjectMapper();

        Flux<ObjectNode> rightChecked = right.flatMapMany(list -> fromIterableChecked(list));
        Flux<ObjectNode> leftChecked = left.transform(flux -> checked(flux));

        
        return Mono.just(
            Tuples.of(leftChecked, rightChecked)
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