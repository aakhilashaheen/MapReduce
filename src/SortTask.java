import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.Buffer;
import java.time.Instant;
import java.util.*;

public class SortTask extends Thread{
    private List<String> intermediateFiles;
    private String outputFile;

    public SortTask(List<String> iFs) {
        this.intermediateFiles = iFs;
    }

    @Override
    public void run() {
        sortFiles();
    }

    public void sortFiles() {
        LinkedList<Pair<String, Integer>> files = new LinkedList<Pair<String, Integer>>();
        for(String filename : intermediateFiles)
            try {
                FileReader fr = new FileReader(filename);
                BufferedReader br = new BufferedReader(fr);
                String line = br.readLine();
                int idx = line.indexOf(",");
                Pair<String, Integer> pair = new Pair<>(line.substring(0, idx), Integer.parseInt(line.substring(idx+1)));
                files.add(pair);
            } catch (Exception e) { }
        Collections.sort(files, (p1, p2) -> { return p1.second - p2.second; });
        this.outputFile = "output_dir/" + Instant.now().toString() + ".txt";
        try {
            FileWriter fw = new FileWriter(this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            for(Pair<String, Integer> pair : files)
                fw.write(pair.first + "\n");
        } catch (Exception e) { }
    }

    public String getOutputFile() {
        return this.outputFile;
    }
}