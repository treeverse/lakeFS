package io.lakefs;

import java.util.Optional;

public final class ImmutablePagination implements FSTestBase.Pagination {
    private final Optional<Integer> amount;
    private final Optional<String> after;
    private final Optional<String> prefix;

    private ImmutablePagination(Optional<Integer> amount, Optional<String> after, Optional<String> prefix) {
        this.amount = amount;
        this.after = after;
        this.prefix = prefix;
    }

    public Optional<Integer> amount() {
        return this.amount;
    }

    public Optional<String> after() {
        return this.after;
    }

    public Optional<String> prefix() {
        return this.prefix;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<Integer> amount = Optional.empty();
        private Optional<String> after = Optional.empty();
        private Optional<String> prefix = Optional.empty();

        public Builder amount(Integer amount) {
            this.amount = Optional.of(amount);
            return this;
        }

        public Builder after(String after) {
            this.after = Optional.of(after);
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = Optional.of(prefix);
            return this;
        }

        public ImmutablePagination build() {
            return new ImmutablePagination(amount, after, prefix);
        }
    }
}
