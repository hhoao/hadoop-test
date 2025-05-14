package org.hhoao.test.flink.source.user;

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

public class UserIteratorFunction extends FromIteratorFunction<User> {
    public UserIteratorFunction() {
        super(new UserIterator());
    }
}
