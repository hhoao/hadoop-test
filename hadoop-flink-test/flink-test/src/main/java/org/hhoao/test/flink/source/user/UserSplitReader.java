package org.hhoao.test.flink.source.user;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.jetbrains.annotations.Nullable;

/**
 * UserSplitReader
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSplitReader implements SplitReader<User, UserSplit> {
    private int currentCount;

    @Override
    public RecordsWithSplitIds<User> fetch() throws IOException {
        User user =
                new User(currentCount, "test" + currentCount, (double) currentCount, currentCount);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        currentCount++;
        return new UserRecordsWithSplitIds(user);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<UserSplit> splitsChange) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {}

    private static class UserRecordsWithSplitIds implements RecordsWithSplitIds<User> {
        private String currentSplit;
        private User currentRecord;

        private UserRecordsWithSplitIds(User currentRecord) {
            this.currentRecord = currentRecord;
            this.currentSplit = "";
        }

        @Override
        public @Nullable String nextSplit() {
            String split = currentSplit;
            currentSplit = null;
            return split;
        }

        @Override
        public @Nullable User nextRecordFromSplit() {
            User record = currentRecord;
            currentRecord = null;
            return record;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
