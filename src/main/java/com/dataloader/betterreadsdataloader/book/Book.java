package com.dataloader.betterreadsdataloader.book;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.LocalDate;
import java.util.List;

@Getter
@Setter
@ToString
@Table(value = "book_by_id")
public class Book {
    @Id
    @PrimaryKeyColumn(name = "book_id",ordinal = 0,type = PrimaryKeyType.PARTITIONED)
    private  String id;

    @Column("book_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String name;

    @Column("published_date")
    @CassandraType(type = CassandraType.Name.DATE)
    private LocalDate publishedDate;

    @Column("book_description")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String description;

    @Column("author_names")
    @CassandraType(type = CassandraType.Name.LIST,typeArguments = CassandraType.Name.TEXT)
    private List<String> authorNames;

    @Column("author_ids")
    @CassandraType(type = CassandraType.Name.LIST,typeArguments = CassandraType.Name.TEXT)
    private List<String> authorIds;

    @Column("cover_ids")
    @CassandraType(type = CassandraType.Name.LIST,typeArguments = CassandraType.Name.TEXT)
    private List<String> coverIds;
}
