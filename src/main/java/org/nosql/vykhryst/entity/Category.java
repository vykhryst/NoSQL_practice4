package org.nosql.vykhryst.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Category {

    private String id;

    private String name;

    public Category(String id) {
        this.id = id;
    }

}
