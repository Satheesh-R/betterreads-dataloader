package com.dataloader.betterreadsdataloader.author;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Getter
@Setter
@ToString
@Table(value = "author_by_id")
public class Author {
    @Id @PrimaryKeyColumn(name="author_id", ordinal = 0,type = PrimaryKeyType.PARTITIONED)
    private String id;
    @CassandraType(type = CassandraType.Name.TEXT)
    private String name;
    @CassandraType(type = CassandraType.Name.TEXT)
    private String penName;
}
