package hdfsGz2Bz2;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

import java.util.*;


@Slf4j
public class HdfsGz2Bz2 {

    @SneakyThrows
    private static Map<String, String> getArgs(String[] args) {
        Map<String, String> mapArgs = new HashMap<>();
        Options options = new Options();
        options.addOption("h", "host", true, "-h input host of hdfs.")
                .addOption("p", "path", true, "-p input path of hdfs.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        //TBD
        options.getOptions().forEach(x -> System.out.println(x.getDescription()));
        if (cmd.hasOption("h") && StringUtils.startsWith(cmd.getOptionValue("h"), "hdfs://")) {
            mapArgs.put("h", cmd.getOptionValue("h"));
        }
        if (cmd.hasOption("p")) {
            mapArgs.put("p", cmd.getOptionValue("p"));
        }
        return mapArgs;
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Map<String, String> mapArgs = getArgs(args);
        if (mapArgs.size() != 2) {
            System.out.println("please input correct parameters !");
            System.exit(-1);
        }

        Convert convert = new Convert(mapArgs.get("h"), mapArgs.get("p"));
        convert.convert();
    }

}


