# Reactor Relational Operators

A reactive Java library providing relational join operator implementations for streaming data processing with Project Reactor.

## Overview

This library implements three classical relational join algorithms optimized for reactive streams, supporting Jackson `ObjectNode` data binding. Choose the appropriate operator based on your data characteristics and memory constraints.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Join Operators](#join-operators)
- [Usage Examples](#usage-examples)
- [Performance Considerations](#performance-considerations)
- [Requirements](#requirements)
- [License](#license)

## Features

- **Reactive streams** support via Project Reactor
- **Three join algorithms**: Nested Loop Join, Sorted Merge Join, and Hash Match Join
- **Multiple join types**: Inner Join, Left Outer Join, Right Outer Join, Full Outer Join, Left Semi Join, Right Semi Join
- **Memory-efficient** processing for large datasets
- **Jackson ObjectNode** integration out of the box
- **Type-safe** operations with configurable join conditions

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.d-sch</groupId>
    <artifactId>reactor-relational-operators</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### Gradle

```gradle
implementation 'io.github.d-sch:reactor-relational-operators:0.0.1-SNAPSHOT'
```

## Join Operators

### Nested Loop Join

Best for small right-side datasets that fit in memory.

-**Characteristics:**
    - No constraints on left dataset size (streamed)
    - Right dataset must fit in memory (materialized as list)
    - Time complexity: O(n × m)
    - Space complexity: O(m) - entire right dataset in memory

- **Supported join types:**
    -Inner Join, Left Outer Join

- **When to use:** 
    -Small lookup tables or dimension data on the right side.

### Sorted Merge Join

Best for pre-sorted datasets, most memory-efficient.

- **Characteristics:**
    - No size constraints on either dataset (both streamed)
    - Requires pre-sorted left and right datasets on join key(s)
    - Time complexity: O(n + m)
    - Space complexity: O(k₁ + k₂) - where k₁ and k₂ are the largest groups of records with matching join keys from left and right datasets respectively

- **Supported join types:** 
    -Inner Join, Left Outer Join, Right Outer Join, Full Outer Join, Left Semi Join, Right Semi Join

- **When to use:** 
    - Already sorted data or when memory is extremely limited. Most efficient when join keys have high cardinality (few duplicates).

**Note:** In the best case with unique keys, space complexity approaches O(1). In the worst case where all records share the same key, it degrades to O(n + m).

### Hash Match Join

Best for large datasets with sufficient memory for hash table.

- **Characteristics:**
    - No constraints on right dataset size (streamed)
    - Left dataset must fit in memory (materialized in hash table)
    - Time complexity: O(n + m) - average case with good hash distribution
    - Space complexity: O(n) - entire left dataset in hash table

- **Supported join types:** 
    - Inner Join, Left Outer Join, Right Outer Join, Full Outer Join

- **When to use:** 
    - Large unsorted datasets with available memory for hashing the smaller (left) dataset.

## Usage Examples

### Nested Loop Join

```java
import io.github.d_sch.reactor.operators.FluxNestedLoopJoin;
import io.github.d_sch.reactor.common.NodePredicate;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.core.publisher.Flux;

// Prepare your data streams
Flux<ObjectNode> leftStream = // ... your left data source
Flux<ObjectNode> rightStream = // ... your right data source

// Define join predicate
NodePredicate predicate = NodePredicate.equals(
    JsonPointer.valueOf("/a/customerId"),
    JsonPointer.valueOf("/b/customerId")
);

// Inner Join
Flux<ObjectNode> innerJoin = FluxNestedLoopJoin.innerJoin(
    predicate,
    leftStream,
    rightStream.collectList()
);

// Left Outer Join
Flux<ObjectNode> leftOuterJoin = FluxNestedLoopJoin.leftOuterJoin(
    predicate,
    leftStream,
    rightStream.collectList()
);

innerJoin.subscribe(result -> System.out.println(result.toPrettyString()));
```

### Sorted Merge Join

```java
import io.github.d_sch.reactor.operators.FluxMergeJoin;
import io.github.d_sch.reactor.common.NodeComparator;
import io.github.d_sch.reactor.common.NodePredicate;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.core.publisher.Flux;

// Pre-sorted data streams
Flux<ObjectNode> sortedLeft = // ... pre-sorted left data
Flux<ObjectNode> sortedRight = // ... pre-sorted right data

// Define comparator for sorted data
NodeComparator comparator = NodeComparator.comparing(
    JsonPointer.valueOf("/a/customerId"),
    JsonPointer.valueOf("/b/customerId")
);

// Define join predicate
NodePredicate predicate = NodePredicate.alwaysTrue();

// Inner Join
Flux<ObjectNode> innerJoin = FluxMergeJoin.innerJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

// Left Outer Join
Flux<ObjectNode> leftOuterJoin = FluxMergeJoin.leftOuterJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

// Full Outer Join
Flux<ObjectNode> fullOuterJoin = FluxMergeJoin.fullOuterJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

// Right Outer Join
Flux<ObjectNode> rightOuterJoin = FluxMergeJoin.rightOuterJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

// Left Semi Join
Flux<ObjectNode> leftSemiJoin = FluxMergeJoin.leftSemiJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

// Right Semi Join
Flux<ObjectNode> rightSemiJoin = FluxMergeJoin.rightSemiJoin(
    comparator,
    predicate,
    sortedLeft,
    sortedRight
);

innerJoin.subscribe(result -> System.out.println(result.toPrettyString()));
```

### Hash Match Join

```java
import io.github.d_sch.reactor.operators.FluxHashMatchJoin;
import io.github.d_sch.reactor.common.NodeHash;
import io.github.d_sch.reactor.common.NodePredicate;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.core.publisher.Flux;

// Prepare your data streams
Flux<ObjectNode> leftStream = // ... your left data source
Flux<ObjectNode> rightStream = // ... your right data source

// Define hash functions
NodeHash<Integer> hashLeft = NodeHash.hashCode(
    JsonPointer.valueOf("/a/customerId")
);

NodeHash<Integer> hashRight = NodeHash.hashCode(
    JsonPointer.valueOf("/b/customerId")
);

// Define join predicate
NodePredicate predicate = NodePredicate.equals(
    JsonPointer.valueOf("/a/customerId"),
    JsonPointer.valueOf("/b/customerId")
);

// Inner Join
Flux<ObjectNode> innerJoin = FluxHashMatchJoin.innerJoin(
    hashLeft,
    hashRight,
    predicate,
    leftStream,
    rightStream
);

// Left Outer Join
Flux<ObjectNode> leftOuterJoin = FluxHashMatchJoin.leftOuterJoin(
    hashLeft,
    hashRight,
    predicate,
    leftStream,
    rightStream
);

// Right Outer Join
Flux<ObjectNode> rightOuterJoin = FluxHashMatchJoin.rightOuterJoin(
    hashLeft,
    hashRight,
    predicate,
    leftStream,
    rightStream
);

// Full Outer Join
Flux<ObjectNode> fullOuterJoin = FluxHashMatchJoin.fullOuterJoin(
    hashLeft,
    hashRight,
    predicate,
    leftStream,
    rightStream
);

innerJoin.subscribe(result -> System.out.println(result.toPrettyString()));
```

## Performance Considerations

| Operator | Left Size | Right Size | Memory Usage | Requires Sorting | Best Use Case |
|----------|-----------|------------|--------------|------------------|---------------|
| Nested Loop | Unlimited (streamed) | Limited (materialized) | Medium (right data) | No | Small lookup tables |
| Sorted Merge | Unlimited (streamed) | Unlimited (streamed) | Low (group buffer)* | Yes | Pre-sorted streams |
| Hash Match | Limited (materialized) | Unlimited (streamed) | High (left data) | No | Large right, hashable left |

\* Memory usage for Sorted Merge Join depends on the distribution of duplicate keys. With high cardinality (unique or few duplicates), memory usage is minimal.

## Requirements

- Java 25 or higher
- Project Reactor 3.7.6
- Jackson Databind 2.19.0
- Lombok 1.18.38
- Hibernate Validator 9.0.1.Final

## License

This project is licensed under the [Apache License 2.0](LICENSE).

Copyright 2023 d-sch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.