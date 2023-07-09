package io.github.d_sch.reactor.common;

import java.util.function.Consumer;
import java.util.function.Predicate;

public final class Predicates {
	public static <T> void set(T value, Predicate<T> condition, Consumer<T> action) {
		if (condition.test(
				value
		)) {
			action.accept(
					value
			);
		}
	}
}
