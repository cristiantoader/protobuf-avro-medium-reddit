package org.ctoader.medium.dao;

import lombok.extern.slf4j.Slf4j;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import java.util.stream.Stream;

@Repository
@Slf4j
public class RedditDao {

    private static final int FETCH_SIZE = 10_000;
    private static final String SQL_FIND_ALL = "select * from May2015";

    private final DataSource dataSource;

    @Autowired
    public RedditDao(DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;

        DatabaseMetaData md = dataSource.getConnection().getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        while (rs.next()) {
            System.out.println(rs.getString(3));
        }
    }


    public Stream<RedditComment> findAll(Function<Record, RedditComment> rowMapper) {
        try {
            return DSL.using(dataSource.getConnection())
                    .resultQuery(SQL_FIND_ALL)
                    .fetchSize(FETCH_SIZE)
                    .stream()
                    .map(rowMapper);

        } catch (Exception e) {
            log.error("", e);
            return Stream.empty();
        }
    }
}
