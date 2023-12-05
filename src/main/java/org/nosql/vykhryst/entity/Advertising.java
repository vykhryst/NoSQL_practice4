package org.nosql.vykhryst.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Advertising {

    private String id;
    private Category category;
    private String name;
    private String measurement;
    private BigDecimal unitPrice;

    private String description;

    private LocalDateTime updatedAt;

    public Advertising(Builder advertisingBuilder) {
        this.id = advertisingBuilder.id;
        this.category = advertisingBuilder.category;
        this.name = advertisingBuilder.name;
        this.measurement = advertisingBuilder.measurement;
        this.unitPrice = advertisingBuilder.unitPrice;
        this.description = advertisingBuilder.description;
        this.updatedAt = advertisingBuilder.updatedAt;
    }


    public static class Builder {
        private String id;
        private Category category;
        private String name;
        private String measurement;
        private BigDecimal unitPrice;
        private String description;
        private LocalDateTime updatedAt;


        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder category(Category category) {
            this.category = category;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder measurement(String measurement) {
            this.measurement = measurement;
            return this;
        }

        public Builder unitPrice(BigDecimal unitPrice) {
            this.unitPrice = unitPrice;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder updatedAt(LocalDateTime updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Advertising build() {
            return new Advertising(this);
        }
    }
}

