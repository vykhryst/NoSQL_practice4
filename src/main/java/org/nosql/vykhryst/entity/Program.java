package org.nosql.vykhryst.entity;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Data
public class Program {
    private String id;
    private Client client;
    private String campaignTitle;
    private String description;
    private LocalDateTime createdAt;
    private Map<Advertising, Integer> advertisings;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Program{\n")
                .append("id: ").append(id).append("\n")
                .append("client: ").append("\n\t")
                .append(client).append("\n")
                .append("campaignTitle: ").append(campaignTitle).append("\n")
                .append("description: ").append(description).append("\n")
                .append("createdAt: ").append(createdAt).append("\n")
                .append("advertisingList: ").append("\n");
        for (Map.Entry<Advertising, Integer> entry : advertisings.entrySet()) {
            sb.append("\t").append(entry.getKey()).append("\n\t")
                    .append("quantity: ").append(entry.getValue()).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

    public Program() {
        advertisings = new HashMap<>();
    }

    public Program(Builder builder) {
        this.id = builder.id;
        this.client = builder.client;
        this.campaignTitle = builder.campaignTitle;
        this.description = builder.description;
        this.createdAt = builder.createdAt;
        this.advertisings = builder.advertising;
    }

    public void addAdvertising(Advertising advertising, Integer quantity) {
        this.advertisings.put(advertising, quantity);
    }

    public void deleteAdvertising(Advertising advertising) {
        this.advertisings.remove(advertising);
    }

    public static class Builder {
        private String id;
        private Client client;
        private String campaignTitle;
        private String description;
        private LocalDateTime createdAt;
        private final Map<Advertising, Integer> advertising = new HashMap<>();


        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder client(Client client) {
            this.client = client;
            return this;
        }

        public Builder campaignTitle(String campaignTitle) {
            this.campaignTitle = campaignTitle;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder createdAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder addAdvertising(Advertising advertising, Integer quantity) {
            this.advertising.put(advertising, quantity);
            return this;
        }

        public Program build() {
            return new Program(this);
        }
    }
}
