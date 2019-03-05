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
        LinkedList<Pair<String, Double>> files = new LinkedList<Pair<String, Double>>();
        for(String filename : intermediateFiles) {
            try {
                FileReader fr = new FileReader("intermediate_dir/" + filename);
                BufferedReader br = new BufferedReader(fr);
                //System.out.println("Reading file " + filename);
                String line = br.readLine();
                int idx = line.indexOf(",");
                Pair<String, Double> pair = new Pair<>(line.substring(0, idx), Double.parseDouble(line.substring(idx + 1)));
                files.add(pair);
                //System.out.println(pair.toString());
            } catch (Exception e) { }
        }
        Collections.sort(files, (p1, p2) -> (int) Math.signum(p1.second - p2.second));
        this.outputFile = "output_dir/" + Instant.now().toString() + ".txt";
        try {
            FileWriter fw = new FileWriter(this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            for(Pair<String, Double> pair : files) {
                System.out.println(pair.toString());
                bw.write(pair.first + "\n");
            }
            bw.close();
        } catch (Exception e) { }
    }

    public String getOutputFile() {
        return this.outputFile;
    }
}