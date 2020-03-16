package org.ctoader.medium;

import lombok.extern.slf4j.Slf4j;
import org.ctoader.medium.dao.RedditComment;
import org.ctoader.medium.dao.RedditCommentMapper;
import org.ctoader.medium.dao.RedditDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Component
@Slf4j
public class ProtobufExporterRunner implements ApplicationRunner {

    private final RedditDao redditDao;

    private static final AtomicLong counter = new AtomicLong(0);
    private static final long BATCH_SIZE = 100_000;

    @Autowired
    public ProtobufExporterRunner(RedditDao redditDao) {
        this.redditDao = redditDao;
    }

    @Override
    public void run(ApplicationArguments args) {

        long totalBytes = redditDao.findAll(RedditCommentMapper::mapRow)
                .parallel()
                .map(ProtobufExporterRunner::makeProtobufRedditComment)
                .map(Reddit.Comment::toByteArray)
                .mapToLong(bytes -> bytes.length)
                .sum();

        log.info("Total size in bytes of protobuf conversion {}.", totalBytes);

    }

    private static Reddit.Comment makeProtobufRedditComment(RedditComment redditComment) {
        if (counter.incrementAndGet() % BATCH_SIZE == 0) {
            log.info("Reached count {}.", counter.get());
        }

        Reddit.Comment.Builder builder = Reddit.Comment.newBuilder();
        safePopulate(builder::setCreatedUtc, redditComment::getCreatedUtc);
        safePopulate(builder::setUps, redditComment::getUps);
        safePopulate(builder::setSubredditId, redditComment::getSubredditId);
        safePopulate(builder::setLinkId, redditComment::getLinkId);
        safePopulate(builder::setName, redditComment::getName);
        safePopulate(builder::setScoreHidden, redditComment::getScoreHidden);
        safePopulate(builder::setAuthorFlairCssClas, redditComment::getAuthorFlairCssClass);
        safePopulate(builder::setAuthorFlairText, redditComment::getAuthorFlairText);
        safePopulate(builder::setSubreddit, redditComment::getSubreddit);
        safePopulate(builder::setId, redditComment::getId);
        safePopulate(builder::setRemovalReason, redditComment::getRemovalReason);
        safePopulate(builder::setGilded, redditComment::getGilded);
        safePopulate(builder::setDowns, redditComment::getDowns);
        safePopulate(builder::setArchived, redditComment::getArchived);
        safePopulate(builder::setAuthor, redditComment::getAuthor);
        safePopulate(builder::setScore, redditComment::getScore);
        safePopulate(builder::setRetrievedOn, redditComment::getRetrievedOn);
        safePopulate(builder::setBody, redditComment::getBody);
        safePopulate(builder::setDistinguished, redditComment::getDistinguished);
        safePopulate(builder::setEdited, redditComment::getEdited);
        safePopulate(builder::setControversiality, redditComment::getControversiality);
        safePopulate(builder::setParentId, redditComment::getParentId);
        return builder.build();
    }

    private static <T> void safePopulate(Consumer<T> consumer, Supplier<T> supplier) {
        Optional.ofNullable(supplier.get())
                .ifPresent(consumer::accept);
    }

}
