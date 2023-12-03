package org.nosql.vykhryst.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Category {

    private long id;

    private String name;

    public Category(long id) {
        this.id = id;
    }
}
