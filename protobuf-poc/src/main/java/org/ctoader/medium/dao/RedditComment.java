package org.ctoader.medium.dao;

import lombok.*;

@Data
@Builder
@AllArgsConstructor @NoArgsConstructor
@ToString
public class RedditComment {
    private Long createdUtc;
    private Integer ups;
    private String subredditId;
    private String linkId;
    private String name;
    private Integer scoreHidden;
    private String authorFlairCssClass;
    private String authorFlairText;
    private String subreddit;
    private String id;
    private String removalReason;
    private Integer gilded;
    private Integer downs;
    private Integer archived;
    private String author;
    private Integer score;
    private Integer retrievedOn;
    private String body;
    private String distinguished;
    private Integer edited;
    private Integer controversiality;
    private String parentId;
}
