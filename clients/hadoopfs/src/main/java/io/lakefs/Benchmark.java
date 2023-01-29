package io.lakefs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.http.util.Asserts;

public class Benchmark {
    private LakeFSFileSystem fs;

    public Benchmark(LakeFSFileSystem fs) {
        this.fs = fs;
    }

    public static void main(String[] args) throws IOException {
        int total = Integer.valueOf(args[0]);
        int fileSize = Integer.valueOf(args[1]);
        System.out.println("Initializing");
        Configuration conf = new Configuration();
        conf.set("fs.lakefs.access.key", System.getenv("LAKEFS_ACCESS_KEY_ID"));
        conf.set("fs.lakefs.secret.key", System.getenv("LAKEFS_SECRET_ACCESS_KEY"));
        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");
        conf.set("fs.lakefs.endpoint", System.getenv("LAKEFS_ENDPOINT"));
        String repo = "example-repo";
        System.out.println("Getting FS");

        LakeFSFileSystem fs = (LakeFSFileSystem) new Path("lakefs://" + repo + "/main").getFileSystem(conf);
        System.out.println("Starting flow");
        new Benchmark(fs).fullFlow(repo, total, fileSize, 1);
        // FSDataOutputStream os = fs.create(new Path("lakefs://" + repo +
        // "/main/luli.txt"));
        // os.writeChars("hewlllllo");
        // os.close();
    }

    public Stream<String> getPaths(String repo, int count, int degreeLog) {
        return IntStream.range(0, count).mapToObj(i -> {
            String path = "lakefs://" + repo + "/main/file_" + i + ".txt";
            if (degreeLog > 0) {
                path = pathToRecursive(repo, degreeLog, i, count);
            }
            return path;
        });
    }

    // private static String reverseString(String str){
    // StringBuilder sb=new StringBuilder(str);
    // sb.reverse();
    // return sb.toString();
    // }

    private static String pathToRecursive(String repo, int degreeLog, int i, int total) {
        String formatter = "%0" + String.valueOf(total).length() + "d";
        String iStr = String.format(formatter, i);
        StringBuilder dir = new StringBuilder();
        for (int j = iStr.length() - degreeLog; j >= 0; j -= degreeLog) {
            dir.insert(0, String.format("%0" + degreeLog + "d", Integer.valueOf(iStr.substring(j, j + degreeLog))));
            dir.insert(0, "/");
        }
        String path = "lakefs://" + repo + "/main" + dir.toString() + "/file.txt";
        return path;
    }

    public void resetAndPrintCounters() {
        Map<CounterInterceptor.RequestType, AtomicLong> counters = fs.getLakeFSClient().getInterceptor()
                .resetAndGetCounters();
        counters.forEach((k, v) -> {
            System.out.println(k.getMethod() + "\t" + k.getPath() + "\t" + v.get());
        });
    }

    public void fullFlow(String repo, int count, int fileSize, int degreeLog) {
        Random rand = new Random();
        byte[] bytes = new byte[fileSize];

        // write files
        System.out.println("Writing files");
        getPaths(repo, count, degreeLog).forEach(path -> {
            FSDataOutputStream os = null;
            try {
                os = fs.create(new Path(path));
                BufferedOutputStream bos = new BufferedOutputStream(os);
                rand.nextBytes(bytes);
                bos.write(bytes);
                bos.flush();
                os.flush();
                bos.close();
                os.close();
            } catch (IOException e) {
                if (os != null) {
                    try {
                        os.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
                e.printStackTrace();
                System.out.println("Failed to create file " + path);
            }
        });
        resetAndPrintCounters();

        System.out.println("Reading files");
        // read files
        getPaths(repo, count, degreeLog).forEach(path -> {
            try {
                FSDataInputStream is = fs.open(new Path(path));
                is.readFully(0, bytes);
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to read file " + path);
            }
        });
        resetAndPrintCounters();

        // list files
        System.out.println("Listing files");
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("lakefs://" + repo + "/main/"), degreeLog > 0);
            int actualCount = 0;
            while (it.hasNext()) {
                it.next();
                actualCount++;
            }
            Asserts.check(actualCount == count, "Expected " + count + " files, got " + actualCount);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to list files");
        }
        resetAndPrintCounters();

        // check files exist
        System.out.println("Checking files exist");
        getPaths(repo, count, degreeLog).forEach(path -> {
            try {
                Asserts.check(fs.exists(new Path(path)), "Expected file " + path + " to exist");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to check file " + path + " exists");
            }
        });
        resetAndPrintCounters();

        // check files don't exist
        System.out.println("Checking files do not exist");
        getPaths(repo, count, degreeLog).forEach(path -> {
            String wrongPath = path + ".bkp";
            try {
                Asserts.check(!fs.exists(new Path(wrongPath)), "Expected file " + wrongPath + "not to exist");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to check file " + wrongPath + " doesn't exists");
            }
        });
        resetAndPrintCounters();
        // get file status
        getPaths(repo, count, degreeLog).forEach(path -> {
            try {
                LakeFSFileStatus s = fs.getFileStatus(new Path(path));
                Asserts.check(s.getLen() == fileSize, "Wrong file size");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to check get file status for " + path);
            }
        });

        // rename files
        System.out.println("Renaming files");
        getPaths(repo, count, degreeLog).forEach(path -> {

            String newPath = path.replace(".txt", "_renamed.txt");
            try {
                fs.rename(new Path(path), new Path(newPath));
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to rename file " + path);
            }
        });
        resetAndPrintCounters();

        // delete files
        System.out.println("Deleting files recursively");
        try {
            fs.delete(new Path("lakefs://" + repo + "/main/"), true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to delete files recursively");
        }
        // System.out.println("Deleting files one by one");
        // getPaths(repo, count, degreeLog).forEach(path -> {
        //     String newPath = path.replace(".txt", "_renamed.txt");
        //     try {
        //         fs.delete(new Path(newPath), false);
        //     } catch (IOException e) {
        //         e.printStackTrace();
        //         System.out.println("Failed to delete file " + newPath);
        //     }
        // });
        resetAndPrintCounters();
    }
}
