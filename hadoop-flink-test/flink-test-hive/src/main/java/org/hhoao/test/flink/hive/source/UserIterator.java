package org.hhoao.test.flink.hive.source;

import java.io.Serializable;
import java.util.Iterator;

public class UserIterator implements Iterator<User>, Serializable {
    int currentCount = 0;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public User next() {
        User user =
                new User(currentCount, "test" + currentCount, (double) currentCount, currentCount);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        currentCount++;
        return user;
    }
}
