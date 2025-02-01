package io.lakefs;

public final class Pagination {
    private final Integer amount;
    private final String after;
    private final String prefix;

    private Pagination(Builder builder) {
        this.amount = builder.amount;
        this.after = builder.after;
        this.prefix = builder.prefix;
    }

    public Integer amount() {
        return this.amount;
    }

    public String after() {
        return this.after;
    }

    public String prefix() {
        return this.prefix;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer amount;
        private String after;
        private String prefix;

        public Builder amount(Integer amount) {
            this.amount = amount;
            return this;
        }

        public Builder after(String after) {
            this.after = after;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = after;
            return this;
        }

        public Pagination build() {
            return new Pagination(this);
        }
    }
}
