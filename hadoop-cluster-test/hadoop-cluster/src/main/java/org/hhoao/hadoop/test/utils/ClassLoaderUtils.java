package org.hhoao.hadoop.test.utils;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * TestUtils
 *
 * @author xianxing
 * @since 2024 /4/12
 */
public class ClassLoaderUtils {
    /**
     * Add library dir.
     *
     * @param libraryPath the library path
     * @throws IOException the io exception
     */
    public static void addLibraryDir(String libraryPath) throws IOException {
        try {
            Field field = ClassLoader.class.getDeclaredField("usr_paths");
            field.setAccessible(true);
            String[] paths = (String[]) field.get(null);
            for (String path : paths) {
                if (libraryPath.equals(path)) {
                    return;
                }
            }

            String[] tmp = new String[paths.length + 1];
            System.arraycopy(paths, 0, tmp, 0, paths.length);
            tmp[paths.length] = libraryPath;
            field.set(null, tmp);
        } catch (IllegalAccessException e) {
            throw new IOException("Failedto get permissions to set library path");
        } catch (NoSuchFieldException e) {
            throw new IOException("Failedto get field handle to set library path");
        }
    }
}
