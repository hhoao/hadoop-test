package org.hhoao.test.flink.source.user;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * UserSimpleVersionedSerializer
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSourceEnumStateSerializer
        implements SimpleVersionedSerializer<UserSourceEnumState> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(UserSourceEnumState obj) throws IOException {
        return new byte[0];
    }

    @Override
    public UserSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
