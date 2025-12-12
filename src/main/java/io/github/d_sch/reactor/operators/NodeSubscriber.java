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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;

import com.fasterxml.jackson.databind.node.ObjectNode;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;

public class NodeSubscriber {

	@Getter
	final private AtomicBoolean isCompleted = new AtomicBoolean();
	@Getter
	final protected Queue<ObjectNode> actualValue = new ArrayDeque<>();

	@Getter
	private BaseSubscriber<ObjectNode> subscriber;

	private Consumer<ObjectNode> nextConsumer = objectNode -> {};

	private Runnable completionRunnable = () -> {};

	private Consumer<Throwable> errorConsumer = e -> {};

	private NodeSubscriber() {
	}

	void reset() {
		try {
			this.getActualValue().poll();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(
					e
			);
			this.cancel();
			this.subscriber.onError(
					e
			);
		}
	}

	public void request() {
		request(
				1L
		);
	}

	public void request(long number) {
		try {
			if (!this.getIsCompleted().get() && this.getActualValue().peek() == null) {
				this.subscriber.request(
						1
				);
			}
		} catch (Throwable e) {
			Exceptions.throwIfFatal(
					e
			);
			this.cancel();
			this.subscriber.onError(
					e
			);
		}
	}

	public void cancel() {
		try {
			this.getActualValue().clear();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(
					e
			);
			this.subscriber.onError(
					e
			);
		}
		this.subscriber.cancel();
	}

	static NodeSubscriber buildSubscriber(Consumer<ObjectNode> nextConsumer, Runnable completionRunnable,
			Consumer<Throwable> errorConsumer) {
		NodeSubscriber result = new NodeSubscriber();
		result.nextConsumer = nextConsumer;
		result.completionRunnable = completionRunnable;
		result.errorConsumer = errorConsumer;

		result.subscriber = new BaseSubscriber<>() {

			@Override
			protected void hookOnNext(@NotNull ObjectNode value) {
				try {
					result.getActualValue().offer(
							value
					);
					if (result.nextConsumer != null) {
						result.nextConsumer.accept(
								value
						);
					}
				} catch (Throwable e) {
					Exceptions.throwIfFatal(
							e
					);
					this.cancel();
					this.onError(
							e
					);
				}
			}

			@Override
			protected void hookOnComplete() {
				try {
					result.getIsCompleted().set(
							true
					);
					if (result.completionRunnable != null) {
						result.completionRunnable.run();
					}
				} catch (Throwable e) {
					Exceptions.throwIfFatal(
							e
					);
					this.onError(
							e
					);
				}
			}

			@Override
			protected void hookOnSubscribe(@NotNull Subscription subscription) {
				try {
					subscription.request(
							1
					);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(
							e
					);
					this.onError(
							e
					);
				}
			}

			@Override
			protected void hookOnError(@NotNull Throwable throwable) {
				try {
					result.errorConsumer.accept(
							throwable
					);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(
							e
					);
					this.onError(
							e
					);
				}
			}
		};
		return result;
	}
}
