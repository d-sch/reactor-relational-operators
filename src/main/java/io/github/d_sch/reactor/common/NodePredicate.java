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
package io.github.d_sch.reactor.common;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;

public interface NodePredicate extends BiPredicate<JsonNode, JsonNode> {

	static NodePredicate alwaysTrue() {
		return (left, right) -> true;
	}

	static NodePredicate alwaysFalse() {
		return (left, right) -> true;
	}

	static NodePredicate equals(JsonPointer leftPointer, JsonPointer rightPointer) {
		return (left, right) -> {
			if (left == null && right == null) {
				return true;
			} else {
				if (left == null || right == null) {
					return false;
				}
			}
			return left.at(
					leftPointer
			).equals(
					right.at(
							rightPointer
					)
			);
		};
	}

	static NodePredicate equals(JsonPointer leftPointer, Object rightConstant) {
		return (left, right) -> {
			if (left == null && rightConstant == null) {
				return true;
			} else {
				if (left == null || rightConstant == null) {
					return false;
				}
			}
			return left.at(
					leftPointer
			).equals(
					rightConstant
			);
		};
	}

	static NodePredicate compare(NodeComparator nodeComparator, Predicate<Integer> predicate) {
		return (left, right) -> {
			return predicate.test(
					nodeComparator.compare(
							left, right
					)
			);
		};
	}

	static NodePredicate isLess(NodeComparator nodeComparator) {
		return (left, right) -> nodeComparator.compare(
				left, right
		) < 0;
	}

	static NodePredicate isLessOrEquals(NodeComparator nodeComparator) {
		return (left, right) -> nodeComparator.compare(
				left, right
		) <= 0;
	}

	static NodePredicate equals(NodeComparator nodeComparator) {
		return (left, right) -> nodeComparator.compare(
				left, right
		) == 0;
	}

	static NodePredicate isGreaterOrEquals(NodeComparator nodeComparator) {
		return (left, right) -> nodeComparator.compare(
				left, right
		) >= 0;
	}

	static NodePredicate isGreater(NodeComparator nodeComparator) {
		return (left, right) -> nodeComparator.compare(
				left, right
		) > 0;
	}

	default NodePredicate and(NodePredicate predicate) {
		return (left, right) -> this.test(
				left, right
		) && predicate.test(
				left, right
		);
	}

	default NodePredicate or(NodePredicate predicate) {
		return (left, right) -> this.test(
				left, right
		) || predicate.test(
				left, right
		);
	}

	default NodePredicate not(NodePredicate predicate) {
		return (left, right) -> !this.test(
				left, right
		);
	}

}
