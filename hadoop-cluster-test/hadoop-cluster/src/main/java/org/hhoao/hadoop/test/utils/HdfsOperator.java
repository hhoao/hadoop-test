package org.hhoao.hadoop.test.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

public class HdfsOperator implements Closeable {
    private final Configuration configuration;
    private FileSystem fileSystem;
    private Path pwd = new Path("/");

    public HdfsOperator(Configuration configuration) {
        this.configuration = configuration;
    }

    public void start() throws Exception {
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        currentUser.doAs(
                (PrivilegedAction<Void>)
                        () -> {
                            try {
                                this.fileSystem = FileSystem.get(configuration);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            processCommands();
                            return null;
                        });
    }

    private void processCommands() {
        // More scaffolding that does a simple command line processor
        printHelp();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done) {
            try {
                System.out.printf("\n%s> ", pwd);

                String line = in.readLine();
                if (line == null) {
                    break;
                }

                String command = line.trim();
                String[] parts = command.split("\\s+");
                if (parts.length == 0) {
                    continue;
                }
                String operation = parts[0];
                String[] args = Arrays.copyOfRange(parts, 1, parts.length);
                Command[] values = Command.values();
                Command c = null;
                for (Command value : values) {
                    for (String s : value.getNormalizedName()) {
                        if (s.equals(operation)) {
                            c = value;
                            break;
                        }
                    }
                }
                if (c != null) {
                    switch (c) {
                        case CD:
                            cd(args);
                            break;
                        case LS:
                            list(args);
                            break;
                        case CAT:
                            cat(args);
                            break;
                        case HELP:
                            printHelp();
                            break;
                        case QUIT:
                            done = true;
                            break;
                        case MKDIR:
                            mkdir(args);
                            break;
                        case TOUCH:
                            touch(args);
                            break;
                        case PWD:
                            pwd();
                            break;
                        default:
                    }
                }

                Thread.sleep(1000); // just to allow the console output to catch up
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
                printHelp();
            }
        }
    }

    private void pwd() {
        System.out.println(pwd);
    }

    private void mkdir(String[] args) throws IOException {
        Map<Option, List<String>> optionListMap = parseOptions(args, null);
        List<String> params = optionListMap.get(CommonOption.PARAMS);
        if (params != null) {
            for (String param : params) {
                Path path = new Path(param);
                boolean absolute = path.isAbsolute();
                if (absolute) {
                    fileSystem.mkdirs(new Path(param));
                } else {
                    Path actualPath = new Path(pwd, path);
                    fileSystem.mkdirs(actualPath);
                }
            }
        }
    }

    private void touch(String[] args) throws IOException {
        Map<Option, List<String>> optionListMap = parseOptions(args, null);
        List<String> params = optionListMap.get(CommonOption.PARAMS);
        if (params != null) {
            for (String param : params) {
                Path path = new Path(param);
                boolean absolute = path.isAbsolute();
                if (absolute) {
                    fileSystem.createNewFile(new Path(param));
                } else {
                    Path actualPath = new Path(pwd, path);
                    fileSystem.createNewFile(actualPath);
                }
            }
        }
    }

    private void cd(String[] commands) {
        if (commands.length == 0) {
            printHelp();
            return;
        }
        String path = commands[0];
        if (path.equals("..")) {
            if (pwd.isRoot()) {
                return;
            }
            pwd = pwd.getParent();
        } else {
            if (path.startsWith("/")) {
                pwd = new Path(path);
            } else {
                pwd = new Path(pwd, path);
            }
        }
    }

    private void cat(String[] commands) throws IOException {
        if (commands.length == 0) {
            printHelp();
            return;
        }

        Map<Option, List<String>> optionListMap = parseOptions(commands, CatOptions.values());
        List<String> params = optionListMap.get(CommonOption.PARAMS);
        if (params == null) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        for (String path : params) {
            if (path.startsWith("/")) {
                paths.add(new Path(path));
            } else {
                paths.add(new Path(pwd, path));
            }
        }
        List<String> l = optionListMap.get(CatOptions.L);
        if (l != null) {
            printLogFile(paths);
        } else {
            for (Path path : paths) {
                FSDataInputStream open = fileSystem.open(path);
                byte[] bytes = new byte[1024];
                int read = open.read(bytes);
                while (read != -1) {
                    System.out.print(new String(bytes));
                    read = open.read(bytes);
                }
            }
        }
    }

    private void printLogFile(List<Path> paths) throws IOException {
        ArrayList<FileStatus> fileStatuses = new ArrayList<>();
        for (Path path : paths) {
            fileStatuses.add(fileSystem.getFileStatus(path));
        }
        for (FileStatus fileStatus : fileStatuses) {
            PrintStream out = System.out;
            if (!fileStatus.getPath().getName().endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
                AggregatedLogFormat.LogReader reader =
                        new AggregatedLogFormat.LogReader(configuration, fileStatus.getPath());
                try {

                    DataInputStream valueStream;
                    AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                    valueStream = reader.next(key);

                    while (valueStream != null) {

                        String containerString =
                                "\n\nContainer: " + key + " on " + fileStatus.getPath().getName();
                        out.println(containerString);
                        out.println(StringUtils.repeat("=", containerString.length()));
                        while (true) {
                            try {
                                AggregatedLogFormat.LogReader.readAContainerLogsForALogType(
                                        valueStream, out, fileStatus.getModificationTime());
                            } catch (EOFException eof) {
                                break;
                            }
                        }

                        // Next container
                        key = new AggregatedLogFormat.LogKey();
                        valueStream = reader.next(key);
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    private Map<Option, List<String>> parseOptions(String[] options, Option[] optionsToParsed) {
        HashMap<Option, List<String>> optionsMap = new HashMap<>();
        int paramsCount = 0;
        Option preOption = null;
        for (String option : options) {
            String trimOption = option.trim();
            if (paramsCount > 0) {
                optionsMap.computeIfAbsent(preOption, option1 -> new ArrayList<>()).add(trimOption);
                paramsCount--;
                continue;
            }

            Optional<Option> arg = Optional.empty();
            if (optionsToParsed != null && optionsToParsed.length > 0) {
                arg =
                        Arrays.stream(optionsToParsed)
                                .filter(
                                        optionToParsed ->
                                                trimOption.equals(
                                                        optionToParsed.getNormalizedName()))
                                .findAny();
            }
            if (arg.isPresent()) {
                optionsMap.putIfAbsent(arg.get(), new ArrayList<>());
                paramsCount = arg.get().paramCount();
                preOption = arg.get();
            } else {
                optionsMap
                        .computeIfAbsent(CommonOption.PARAMS, option1 -> new ArrayList<>())
                        .add(trimOption);
            }
        }
        return optionsMap;
    }

    private void list(String[] commands) throws IOException {
        Map<Option, List<String>> options = parseOptions(commands, ListOption.values());

        List<Path> paths = new ArrayList<>();
        List<String> args = options.get(CommonOption.PARAMS);
        if (args != null) {
            for (String arg : args) {
                Path path = new Path(arg);
                if (!path.isAbsolute()) {
                    path = new Path(pwd, path);
                }
                paths.add(path);
            }
        }

        if (paths.isEmpty()) {
            paths.add(pwd);
        }

        boolean details = options.containsKey(ListOption.L);

        boolean recursively = options.containsKey(ListOption.R);
        for (Path path : paths) {
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            FileNode node = new FileNode(fileStatus);
            if (recursively) {
                fillNode(node);
            } else {
                fillNodeOnce(node);
            }
            printFileNode(node, details);
        }
    }

    private void fillNodeOnce(FileNode node) throws IOException {
        if (node.info.isDirectory()) {
            Iterator<FileStatus> iterator =
                    Arrays.stream(fileSystem.listStatus(node.info.getPath())).iterator();
            while (iterator.hasNext()) {
                FileStatus next = iterator.next();
                FileNode fileNode = new FileNode(next);
                node.addChild(fileNode);
            }
        }
    }

    private void printFileNode(FileNode node, boolean details) throws IOException {
        if (node.info.isDirectory()) {
            System.out.println(node.info.getPath().toUri().getPath() + ":");
        } else {
            printFileStatus(node.info, details);
        }
        Map<Boolean, List<FileNode>> collect =
                node.children.stream()
                        .collect(Collectors.groupingBy((child) -> child.info.isFile()));
        List<FileNode> fileNodes = collect.get(true);
        int i = 0;
        if (fileNodes != null && !fileNodes.isEmpty()) {
            for (FileNode child : fileNodes) {
                if (!details) {
                    if (++i % 5 == 0) {
                        System.out.println();
                    }
                }
                printFileNode(child, details);
            }
            System.out.println();
            if (!details) {
                System.out.println();
            }
        }
        List<FileNode> dirNodes = collect.get(false);
        if (dirNodes != null) {
            for (FileNode dirNode : dirNodes) {
                printFileNode(dirNode, details);
            }
        }
    }

    private void printFileStatus(FileStatus info, boolean details) throws MalformedURLException {
        if (details) {
            System.out.printf(
                    "%s\t %s\t %s\t %s\t %s\t %s\t %s\t %s \n",
                    info.isFile() ? "f" : "d",
                    info.getPermission(),
                    info.getReplication(),
                    info.getBlockSize(),
                    info.getOwner(),
                    info.getBlockSize(),
                    info.getModificationTime(),
                    info.getPath().getName());
        } else {
            System.out.printf("%s\t", info.getPath().getName());
        }
    }

    private void fillNode(FileNode root) throws IOException {
        Iterator<FileStatus> iterator =
                Arrays.stream(fileSystem.listStatus(root.info.getPath())).iterator();
        while (iterator.hasNext()) {
            FileStatus next = iterator.next();
            FileNode fileNode = new FileNode(next);
            root.addChild(fileNode);
            if (next.isDirectory()) {
                fillNode(fileNode);
            }
        }
    }

    private static class FileNode {
        protected final FileStatus info;
        protected final List<FileNode> children;

        public FileNode(FileStatus info) {
            this.info = info;
            this.children = new ArrayList<>();
        }

        public void addChild(FileNode child) {
            children.add(child);
        }
    }

    enum Command {
        LS("ls", " [-r] [path]: List path"),
        CAT("cat", "<file>: Cat file"),
        CD("cd", "<path>: Enter path"),
        PWD("pwd", "Show current path"),
        MKDIR("mkdir", "<dir>: Create direction"),
        TOUCH("touch", "<file>: Create file"),
        QUIT(Stream.of("quit", "q").collect(Collectors.toList()), "Quit"),
        HELP(Stream.of("help", "h", "?").collect(Collectors.toList()), "Show help info");

        private final String description;
        private final List<String> normalizedNames;

        Command(List<String> normalizedNames, String description) {

            this.description = description;
            this.normalizedNames = normalizedNames;
        }

        Command(String normalizedNames, String description) {
            this.description = description;
            this.normalizedNames = Collections.singletonList(normalizedNames);
        }

        public String getDescription() {
            return description;
        }

        public List<String> getNormalizedName() {
            return normalizedNames;
        }
    }

    private void printHelp() {
        System.out.println("Help:");
        for (Command value : Command.values()) {
            System.out.printf("%s %s\n", value.getNormalizedName(), value.getDescription());
        }
        System.out.println();
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
    }

    interface Option {
        default String getNormalizedName() {
            return null;
        }

        default int paramCount() {
            return 0;
        }
    }

    enum CommonOption implements Option {
        PARAMS
    }

    enum CatOptions implements Option {
        L("-l");

        private final String normalizedName;
        private final int paramCount;

        CatOptions(String normalizedName) {
            this(normalizedName, 0);
        }

        CatOptions(String normalizedName, int paramCount) {
            this.normalizedName = normalizedName;
            this.paramCount = paramCount;
        }

        @Override
        public String getNormalizedName() {
            return normalizedName;
        }

        @Override
        public int paramCount() {
            return paramCount;
        }
    }

    enum ListOption implements Option {
        R("-r"),
        L("-l");

        private final String normalizedName;
        private final int paramCount;

        ListOption(String normalizedName) {
            this(normalizedName, 0);
        }

        ListOption(String normalizedName, int paramCount) {
            this.normalizedName = normalizedName;
            this.paramCount = paramCount;
        }

        @Override
        public String getNormalizedName() {
            return normalizedName;
        }

        @Override
        public int paramCount() {
            return paramCount;
        }
    }
}
