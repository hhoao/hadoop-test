package org.hhoao.test.flink.source.user;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * UserSourceReader
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSourceReader
        extends SingleThreadMultiplexSourceReaderBase<User, User, UserSplit, UserSplitState> {

    public UserSourceReader(
            Supplier<SplitReader<User, UserSplit>> splitReaderSupplier,
            RecordEmitter<User, User, UserSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, UserSplitState> map) {}

    @Override
    protected UserSplitState initializedState(UserSplit userSplit) {
        return null;
    }

    @Override
    protected UserSplit toSplitType(String s, UserSplitState userSplitState) {
        return null;
    }
}
