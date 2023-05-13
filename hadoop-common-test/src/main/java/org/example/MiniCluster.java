package org.example;

public interface MiniCluster {

    public void start() throws Exception;

    public void stop() throws Exception;

    public void stop(boolean cleanUp) throws Exception;

    public void configure() throws Exception;

    public void cleanUp() throws Exception;

}
