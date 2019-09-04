package hdfsGz2Bz2;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @program: hdfsGz2Bz2
 * @description:
 * @author: lin wang
 * @create: 2019-09-04
 **/

@Slf4j
class Convert {
    private final String host;
    private final String path;

    Convert(String host, String path) {
        this.host = host;
        this.path = path;
    }

    @SneakyThrows
    void convert() {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(host), configuration);
        log.info("Start to convert ");
        long allStartTime = System.currentTimeMillis();
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));

        ExecutorService executors = Executors.newCachedThreadPool();
        List<Callable> tasks = Arrays.stream(fileStatuses)
                .map((file) -> {
                    return (Callable) () -> {
                        covertSingleFile(configuration, fs, file);
                        return null;
                    };
                })
                .collect(Collectors.toList());
        List<Future> futures = tasks.stream()
                .map((Function<Callable, Future>) executors::submit)
                .collect(Collectors.toList());
        for (Future future : futures) {
            future.get();
        }
        long allFinishTime = System.currentTimeMillis();
        log.info("Finish to convert.");
        log.info("All finish time is :" + (allFinishTime - allStartTime) / 1000);
        log.info("All procedures are done. Bye !");
        executors.shutdown();
    }

    private void covertSingleFile(Configuration configuration, FileSystem fs, FileStatus file) throws IOException {

        if (!file.getPath().getName().endsWith(".gz")) {
            return;
        }
        log.info(String.join(" : ", file.getPath().getName(), "start~"));

        long start = System.currentTimeMillis();

        String outPath = path + Path.SEPARATOR + StringUtils.replace(file.getPath().getName(), ".gz", ".bz2");
        if (fs.exists(new Path(outPath))) {
            fs.delete(new Path(outPath), false);
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codecGzip = factory.getCodecByName("gzip");
        CompressionCodec codecBz2 = factory.getCodecByName("bzip2");
        InputStream inputStream = codecGzip.createInputStream(fs.open(file.getPath()));
        OutputStream outputStream = codecBz2.createOutputStream(fs.create(new Path(outPath)));
        IOUtils.copyBytes(inputStream, outputStream, configuration);

        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);

        fs.delete(file.getPath(), false);

        long stop = System.currentTimeMillis();
        long time = (stop - start) / 1000;
        log.info(String.join(" : ", file.getPath().getName(), "Finish with " + time + " sec"));
    }
}

