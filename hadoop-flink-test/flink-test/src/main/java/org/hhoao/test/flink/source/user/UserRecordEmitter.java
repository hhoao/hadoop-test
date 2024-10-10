package org.hhoao.test.flink.source.user;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * UserRecordEmitter
 *
 * @author w
 * @since 2024/10/10
 */
public class UserRecordEmitter implements RecordEmitter<User, User, UserSplitState> {
    @Override
    public void emitRecord(
            User user, SourceOutput<User> sourceOutput, UserSplitState userSplitState)
            throws Exception {
        sourceOutput.collect(user);
    }
}
