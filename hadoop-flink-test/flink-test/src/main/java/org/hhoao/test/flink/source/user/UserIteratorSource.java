package org.hhoao.test.flink.source.user;

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

public class UserIteratorSource extends FromIteratorFunction<User> {
    public UserIteratorSource() {
        super(new UserIterator());
    }
}
