package org.hhoao.test.flink.source.user;

import java.io.IOException;
import java.util.*;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.jetbrains.annotations.Nullable;

/**
 * UserSourceSplitEnumerator
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSourceSplitEnumerator implements SplitEnumerator<UserSplit, UserSourceEnumState> {
    private final SplitEnumeratorContext<UserSplit> context;

    public UserSourceSplitEnumerator(SplitEnumeratorContext<UserSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addReader(int subtaskId) {
        Map<Integer, List<UserSplit>> incrementalAssignment = new HashMap<>();
        ArrayList<UserSplit> userSplits = new ArrayList<>();
        userSplits.add(new UserSplit());
        incrementalAssignment.put(subtaskId, userSplits);
        context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
    }

    @Override
    public UserSourceEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List splits, int subtaskId) {}
}
