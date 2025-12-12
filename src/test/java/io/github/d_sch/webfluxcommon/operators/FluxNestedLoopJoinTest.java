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
package io.github.d_sch.webfluxcommon.operators;

import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodePredicate;
import io.github.d_sch.reactor.operators.FluxNestedLoopJoin;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FluxNestedLoopJoinTest {

    private static ObjectMapper om = JsonMapper.builder().build();
    private Flux<ObjectNode> left;
    private Flux<ObjectNode> right;

    @BeforeEach
    public void beforeEach() throws JsonMappingException, JsonProcessingException {

        left = Mono.just(
                Arrays.asList(
                        om.readTree(
                                "{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 1}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"5\", \"numberOfInquiries\": 2}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"2\", \"numberOfInquiries\": 4}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"6\", \"numberOfInquiries\": 5}}"
                        )
                )
        ).flatMapMany(
                list -> Flux.fromIterable(
                        list
                ).cast(
                        ObjectNode.class
                )
        );

        System.out.println(
                "LEFT: " + om.valueToTree(
                        left.collectList().block()
                ).toPrettyString()
        );

        right = Mono.just(
                Arrays.asList(
                        om.readTree(
                                "{\"b\": {\"customerId\": \"1\", \"numberOfInquiries\": 3}}"
                        ), om.readTree(
                                "{\"b\": {\"customerId\": \"3\", \"numberOfInquiries\": 6}}"
                        ), om.readTree(
                                "{\"b\": {\"customerId\": \"5\", \"numberOfInquiries\": 7}}"
                        )
                )
        ).flatMapMany(
                list -> Flux.fromIterable(
                        list
                ).cast(
                        ObjectNode.class
                )
        );

        System.out.println(
                "RIGHT: " + om.valueToTree(
                        right.collectList().block()
                ).toPrettyString()
        );

    }

    @Test
    public void testInnerJoin() throws JsonMappingException, JsonProcessingException {        

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        StepVerifier.create(
                FluxNestedLoopJoin.innerJoin(
                        predicate, left, right.collectList()
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                    result.forEach(item -> System.out.println("INNER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testLeftOuterJoin() throws JsonMappingException, JsonProcessingException {        

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        StepVerifier.create(
                FluxNestedLoopJoin.leftOuterJoin(
                        predicate, left, right.collectList()
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"2\",\"numberOfInquiries\":4}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"6\",\"numberOfInquiries\":5}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                    result.forEach(item -> System.out.println("LEFT_OUTER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }
}
