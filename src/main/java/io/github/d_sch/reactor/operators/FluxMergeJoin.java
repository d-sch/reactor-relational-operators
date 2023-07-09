package io.github.d_sch.reactor.operators;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodeComparator;
import io.github.d_sch.reactor.common.NodePredicate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class FluxMergeJoin {

	MergeJoinType mergeJoinType = MergeJoinType.FULL_OUTER_JOIN;

	String leftMergeName = "left";
	String rightMergeName = "right";

	NodeComparator comparator;

	NodePredicate predicate = (left, right) -> true;
	ObjectMapper om = new ObjectMapper();

	NodeSubscriber leftSubscriber;
	NodeSubscriber rightSubscriber;

	FluxSink<ObjectNode> fluxSink;

	private FluxMergeJoin() {
		leftSubscriber = NodeSubscriber.buildSubscriber(
				this::onNext, this::onCompleted, this::onError
		);
		rightSubscriber = NodeSubscriber.buildSubscriber(
				this::onNext, this::onCompleted, this::onError
		);
	}

	public static Flux<ObjectNode> innerJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.INNER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> leftOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.LEFT_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> fullOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.FULL_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> rightOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.RIGHT_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> leftSemiJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.LEFT_SEMI_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> rightSemiJoin(NodeComparator comparator, NodePredicate predicate,
			Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				MergeJoinType.RIGHT_SEMI_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> mergeJoinSorted(MergeJoinType mergeJoinType, NodeComparator comparator,
			NodePredicate predicate, Mono<List<ObjectNode>> left, Mono<List<ObjectNode>> right) {
		return mergeJoinSorted(
				mergeJoinType, comparator, predicate, left.flatMapMany(
						jsonNodes -> Flux.defer(
								() -> Flux.fromIterable(
										jsonNodes
								)
						)
				), right.flatMapMany(
						jsonNodes -> Flux.defer(
								() -> Flux.fromIterable(
										jsonNodes
								)
						)
				)
		);
	}

	public static Flux<ObjectNode> innerJoin(NodeComparator comparator, NodePredicate predicate, Flux<ObjectNode> left,
			Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.INNER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> leftOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.LEFT_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> fullOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.FULL_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> rightOuterJoin(NodeComparator comparator, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.RIGHT_OUTER_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> leftSemiJoin(NodeComparator comparator, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.LEFT_SEMI_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> rightSemiJoin(NodeComparator comparator, NodePredicate predicate,
			Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return mergeJoinSorted(
				MergeJoinType.RIGHT_SEMI_JOIN, comparator, predicate, left, right
		);
	}

	public static Flux<ObjectNode> mergeJoinSorted(MergeJoinType mergeJoinType, NodeComparator comparator,
			NodePredicate predicate, Flux<ObjectNode> left, Flux<ObjectNode> right) {
		return Flux.create(
				fluxSink -> {
					FluxMergeJoin mergeJoin = new FluxMergeJoin();
					mergeJoin.mergeJoinType = mergeJoinType;
					mergeJoin.comparator = comparator;
					mergeJoin.predicate = predicate;
					mergeJoin.fluxSink = fluxSink;
					fluxSink.onCancel(
							mergeJoin::cancel
					);
					mergeJoin.subscribe(
							left, right
					);
				}, FluxSink.OverflowStrategy.BUFFER
		);
	}

	protected void onCompleted() {
		try {
			checkNext();
			if (leftSubscriber.getIsCompleted().get() && rightSubscriber.getIsCompleted().get()) {
				complete();
			}
		} catch (Throwable e) {
			this.error(
					e
			);
		}
	}

	private static Flux<ObjectNode> checkEmptyObject(Flux<ObjectNode> flux) {
		return flux.<ObjectNode>handle(
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

	public void subscribe(Flux<ObjectNode> left, Flux<ObjectNode> right) {
		try {
			left.transform(
					FluxMergeJoin::checkEmptyObject
			).subscribe(
					this.leftSubscriber.getSubscriber()
			);
			right.transform(
					FluxMergeJoin::checkEmptyObject
			).subscribe(
					this.rightSubscriber.getSubscriber()
			);
		} catch (Throwable e) {
			this.cancel();
			this.error(
					e
			);
		}
	}

	protected void complete() {
		try {
			if (!leftSubscriber.getIsCompleted().get() || !rightSubscriber.getIsCompleted().get()) {
				cancel();
			}
			fluxSink.complete();
		} catch (Throwable e) {
			this.error(
					e
			);
		}
	}

	public void cancel() {
		try {
			if (!leftSubscriber.getIsCompleted().get()) {
				leftSubscriber.cancel();
			}
			if (!rightSubscriber.getIsCompleted().get()) {
				rightSubscriber.cancel();
			}
		} catch (Throwable e) {
			fluxSink.error(
					e
			);
		}
	}

	protected void onNext(ObjectNode value) {
		try {
			checkNext();
		} catch (Throwable e) {
			this.cancel();
			fluxSink.error(
					e
			);
		}
	}

	protected void onError(Throwable throwable) {
		fluxSink.error(
				throwable
		);
	}

	protected void error(Throwable e) {
		fluxSink.error(
				e
		);
	}

	protected void checkNext() {
		if ((leftSubscriber.getActualValue().peek() != null || leftSubscriber.getIsCompleted().get())
				&& (rightSubscriber.getActualValue().peek() != null || rightSubscriber.getIsCompleted().get())) {
			handleNext(
					leftSubscriber.getActualValue().peek(), rightSubscriber.getActualValue().peek()
			);
			if ((leftSubscriber.getIsCompleted().get() && mergeJoinType == MergeJoinType.RIGHT_SEMI_JOIN)
				|| (rightSubscriber.getIsCompleted().get() && mergeJoinType == MergeJoinType.LEFT_SEMI_JOIN)) {
					complete();
			}
		}
	}

	private void handleNext(ObjectNode left, ObjectNode right) {
		int compare = left == null ? 1 : right == null ? -1 : 0;
		boolean matched = false;

		if (compare == 0) {
			compare = compare(
					left, right
			);
			matched = compare == 0 && predicateTest(
					left, right
			);
			leftSubscriber.getIsMatched().set(
					matched
			);
			rightSubscriber.getIsMatched().set(
					matched
			);
		}
		if (matched) {
			if (mergeJoinType == MergeJoinType.INNER_JOIN || mergeJoinType == MergeJoinType.FULL_OUTER_JOIN
					|| mergeJoinType == MergeJoinType.LEFT_OUTER_JOIN
					|| mergeJoinType == MergeJoinType.RIGHT_OUTER_JOIN) {
				bothMatchedJoin(
						left, right
				);
			} else {
				if (mergeJoinType == MergeJoinType.LEFT_SEMI_JOIN) {
					next(
							left
					);
				} else if (mergeJoinType == MergeJoinType.RIGHT_SEMI_JOIN) {
					next(
							right
					);
				}
			}
			leftSubscriber.reset();
			rightSubscriber.reset();
		} else {
			if (compare < 0 || compare == 0) {
				if (predicateTest(
						left, null
				) && (mergeJoinType == MergeJoinType.FULL_OUTER_JOIN
						|| mergeJoinType == MergeJoinType.LEFT_OUTER_JOIN)) {
					oneIsLessOrNotMatchedOrOtherCompleted(
							left, null
					);
				}
				leftSubscriber.reset();
			}
			if (compare > 0 || compare == 0) {
				if (predicateTest(
						null, right
				) && (mergeJoinType == MergeJoinType.FULL_OUTER_JOIN
						|| mergeJoinType == MergeJoinType.RIGHT_OUTER_JOIN)) {
					oneIsLessOrNotMatchedOrOtherCompleted(
							null, right
					);
				}
				rightSubscriber.reset();
			}
		}
		if (!(leftSubscriber.getIsCompleted().get() && mergeJoinType == MergeJoinType.RIGHT_SEMI_JOIN)
				&& !(rightSubscriber.getIsCompleted().get() && mergeJoinType == MergeJoinType.LEFT_SEMI_JOIN)) {
			leftSubscriber.request();
			rightSubscriber.request();
		} else {
			leftSubscriber.reset();
			rightSubscriber.reset();
		}
	}

	private int compare(JsonNode left, JsonNode right) {
		return comparator.compare(
				left, right
		);
	}

	private boolean predicateTest(JsonNode left, JsonNode right) {
		return predicate.test(
				left, right
		);
	}

	private void next(ObjectNode merged) {
		fluxSink.next(
				merged
		);
	}

	private void bothMatchedJoin(ObjectNode left, ObjectNode right) {
		next(
				createMergeObjectNode(
						left, right
				)
		);
	}

	private void oneIsLessOrNotMatchedOrOtherCompleted(ObjectNode left, ObjectNode right) {
		if (left == null && right == null) {
			throw new IllegalArgumentException(
					"One of left or right must be a vaild json node!"
			);
		} else if (left != null && right != null) {
			throw new IllegalArgumentException(
					"One of left or right must be null!"
			);
		}
		next(
				createMergeObjectNode(
						left, right
				)
		);
	}

	private ObjectNode createMergeObjectNode(ObjectNode left, ObjectNode right) {
		if (left == null && right == null) {
			throw new IllegalArgumentException(
					"One of left or right must be a vaild json node!"
			);
		}
				
		ObjectNode result = om.createObjectNode();
		if (left != null) {
			result.setAll(
					left
			);
		}
		if (right != null) {
			result.setAll(
					right
			);
		}
		return result;
	}

}
