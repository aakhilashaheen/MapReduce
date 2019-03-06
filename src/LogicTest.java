import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;

public class LogicTest {

    private static HashSet<String> positives, negatives;
    public static void main(String[] args) {
        String directoryToBeProcessed = "test_input_dir";

        File inputDirectory = new File("data/input_dir/"+directoryToBeProcessed);
        if(inputDirectory.isDirectory()){
            System.out.println("Yes");
        } else{
            System.out.println(inputDirectory.exists());
        }
            System.out.println(inputDirectory.getAbsolutePath());


//
//	positives = new HashSet<>();
//	try {
//	    FileReader fr = new FileReader("data/positive.txt");
//	    BufferedReader br = new BufferedReader(fr);
//	    String line = null;
//	    while((line = br.readLine()) != null) {
//		positives.add(line.replaceAll("\n", ""));
//	    }
//	    fr.close();
//	}
//	catch (Exception e) { e.printStackTrace(); }
//
//	negatives = new HashSet<>();
//	try {
//	    FileReader fr = new FileReader("data/negative.txt");
//	    BufferedReader br = new BufferedReader(fr);
//	    String line = null;
//	    while((line = br.readLine()) != null) {
//		negatives.add(line.replaceAll("\n", ""));
//	    }
//	    fr.close();
//	} catch (Exception e) { e.printStackTrace(); }
//
//
//	File[] listOfFiles = (new File("input_dir")).listFiles();
//	ArrayList<String> intermediateFiles = new ArrayList<>(listOfFiles.length);
//	for(File file : listOfFiles) {
//	    MapTask mapTask = new MapTask("input_dir/" + file.getName(), positives, negatives);
//	    mapTask.countSentiment();
//	    intermediateFiles.add(mapTask.getOutputFile());
//	}
//	SortTask sortTask = new SortTask("intermediate_dir");
//	sortTask.sortFiles();
//	String outputFile = sortTask.getOutputFile();
    }

}

