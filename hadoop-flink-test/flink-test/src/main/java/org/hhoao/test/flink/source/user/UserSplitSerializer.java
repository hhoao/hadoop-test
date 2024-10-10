package org.hhoao.test.flink.source.user;

import java.io.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * UserSimpleVersionedSerializer
 *
 * @author w
 * @since 2024/10/10
 */
public class UserSplitSerializer implements SimpleVersionedSerializer<UserSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(UserSplit obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public UserSplit deserialize(int version, byte[] serialized) throws IOException {
        return new UserSplit();
    }
}
