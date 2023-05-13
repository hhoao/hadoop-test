package org.hhoao.hadoop.test.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/** Utility functions for jar files. */
public class JarUtils {

    public static void checkJarFile(URL jar) throws IOException {
        File jarFile;
        try {
            jarFile = new File(jar.toURI());
        } catch (URISyntaxException e) {
            throw new IOException("JAR file path is invalid '" + jar + '\'');
        }
        if (!jarFile.exists()) {
            throw new IOException("JAR file does not exist '" + jarFile.getAbsolutePath() + '\'');
        }
        if (!jarFile.canRead()) {
            throw new IOException("JAR file can't be read '" + jarFile.getAbsolutePath() + '\'');
        }

        try (JarFile ignored = new JarFile(jarFile)) {
            // verify that we can open the Jar file
        } catch (IOException e) {
            throw new IOException(
                    "Error while opening jar file '" + jarFile.getAbsolutePath() + '\'', e);
        }
    }

    public static List<URL> getJarFiles(final String[] jars) {
        if (jars == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(jars)
                .map(
                        jarPath -> {
                            try {
                                final URL fileURL =
                                        new File(jarPath).getAbsoluteFile().toURI().toURL();
                                JarUtils.checkJarFile(fileURL);
                                return fileURL;
                            } catch (MalformedURLException e) {
                                throw new IllegalArgumentException("JAR file path invalid", e);
                            } catch (IOException e) {
                                throw new RuntimeException("Problem with jar file " + jarPath, e);
                            }
                        })
                .collect(Collectors.toList());
    }
}
