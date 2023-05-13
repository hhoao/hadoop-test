package org.hhoao.hadoop.test.utils;

import java.io.File;
import java.net.URL;

/** The type Resources. */
public class Resources {
    /**
     * Gets resource.
     *
     * @param name the name
     * @return the resource
     */
    public static URL getResource(String name) {
        return Resources.class.getClassLoader().getResource(name);
    }

    /**
     * Gets resource for root.
     *
     * @param name the name
     * @return the resource for root
     */
    public static URL getResourceForRoot(String name) {
        return Resources.class.getClassLoader().getResource(name);
    }

    /**
     * Gets resource root.
     *
     * @return the resource root
     */
    public static URL getResourceRoot() {
        return Resources.class.getResource("/");
    }

    /**
     * Gets project root.
     *
     * @return the project root
     */
    public static String getProjectRoot() {
        return new File("").getAbsolutePath();
    }

    /**
     * Gets target dir.
     *
     * @return the target dir
     */
    public static String getTargetDir() {
        String projectRoot = getProjectRoot();
        return new File(projectRoot, "target").getAbsolutePath();
    }

    /**
     * Create resource dir for root string.
     *
     * @param dir the dir
     * @return the string
     */
    public static String createResourceDirForRoot(String dir) {
        File file = new File(getResourceRoot().getFile(), dir);
        return file.getAbsolutePath();
    }
}
