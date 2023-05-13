package org.example.utils;

import java.io.File;

/**
 * FileUtils
 *
 * @author hhoa
 * @since 2023/4/24
 **/


public class FileUtils {
    public static boolean deleteFile(String dirFile) {
        return deleteFile(new File(dirFile));
    }
    /**
     * 先根遍历序递归删除文件夹
     *
     * @param dirFile 要被删除的文件或者目录
     * @return 删除成功返回true, 否则返回false
     */
    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }
}
