package org.hhoao.test.flink.source.user;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * UserSplit
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSplit implements SourceSplit {
    @Override
    public String splitId() {
        return "";
    }
}
