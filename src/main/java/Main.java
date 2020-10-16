import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Main {
    public static void main(String[] args) throws Exception {

        String hdfsuri = "hdfs://192.168.151.92:8020";

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);


        //==== Create folder if not exists
        Path workingDir=fs.getWorkingDirectory();
        System.out.println("Current WorkDir: "+workingDir);
        Path newFolderPath= new Path(workingDir, "/newFolder");
        System.out.println("New folder path: "+newFolderPath);
        if(!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            System.out.println("Path "+newFolderPath+" created.");
        }

        //Write file
        //Create a path
        Path hdfswritepath = new Path(newFolderPath + "/" + "testFile.csv");
        //Init output stream
        FSDataOutputStream outputStream=fs.create(hdfswritepath);
        //Cassical output stream usage
        outputStream.writeBytes("2;amortecedor;teste;cofap");
        outputStream.close();

        //Read file
        //Create a path
        Path hdfsreadpath = new Path(newFolderPath + "/" + "testFile.csv");
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        System.out.println(out);
        inputStream.close();

    }
}
