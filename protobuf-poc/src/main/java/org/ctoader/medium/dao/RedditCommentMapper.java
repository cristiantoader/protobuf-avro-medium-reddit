package org.ctoader.medium.dao;

import org.jooq.Record;

public class RedditCommentMapper {

    public static RedditComment mapRow(Record record) {
        return RedditComment.builder()
                .createdUtc(record.getValue("created_utc", Long.class))
                .ups(record.getValue("ups", Integer.class))
                .subredditId(record.getValue("subreddit_id", String.class))
                .linkId(record.getValue("link_id", String.class))
                .name(record.getValue("name", String.class))
                .scoreHidden(record.getValue("score_hidden", Integer.class))
                .authorFlairCssClass(record.getValue("author_flair_css_class", String.class))
                .authorFlairText(record.getValue("author_flair_text", String.class))
                .subreddit(record.getValue("subreddit", String.class))
                .id(record.getValue("id", String.class))
                .removalReason(record.getValue("removal_reason", String.class))
                .gilded(record.getValue("gilded", Integer.class))
                .downs(record.getValue("downs", Integer.class))
                .archived(record.getValue("archived", Integer.class))
                .author(record.getValue("author", String.class))
                .score(record.getValue("score", Integer.class))
                .retrievedOn(record.getValue("retrieved_on", Integer.class))
                .body(record.getValue("body", String.class))
                .distinguished(record.getValue("distinguished", String.class))
                .edited(record.getValue("edited", Integer.class))
                .controversiality(record.getValue("controversiality", Integer.class))
                .parentId(record.getValue("parent_id", String.class))
                .build();
    }
}
