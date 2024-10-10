package org.hhoao.test.flink.source.user;

import java.util.function.Supplier;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class UserSource implements Source<User, UserSplit, UserSourceEnumState> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<UserSplit, UserSourceEnumState> createEnumerator(
            SplitEnumeratorContext<UserSplit> enumContext) throws Exception {
        return new UserSourceSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<UserSplit, UserSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<UserSplit> enumContext, UserSourceEnumState checkpoint)
            throws Exception {
        return new UserSourceSplitEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<UserSplit> getSplitSerializer() {
        return new UserSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<UserSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new UserSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<User, UserSplit> createReader(SourceReaderContext readerContext) {
        Supplier<SplitReader<User, UserSplit>> userSplitReaderSupplier = UserSplitReader::new;
        UserRecordEmitter userRecordEmitter = new UserRecordEmitter();
        Configuration configuration = readerContext.getConfiguration();
        return new UserSourceReader(
                userSplitReaderSupplier, userRecordEmitter, configuration, readerContext);
    }
}
