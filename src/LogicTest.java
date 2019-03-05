import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;

public class LogicTest {

    private static HashSet<String> positives, negatives;
    public static void main(String[] args) {
	//
	//String line = "Whatsdkfjsk.--dsfdsfkje's";
	//String[] words= line.replaceAll("--"," ").split("\\s+");
	//for(String word : words){
	//word = word.replaceAll("^[a-zA-Z],^-","");
	//if(word.contains("'")){
	//System.out.println(word);
	//word = word.substring(0,word.indexOf("'"));
	//}
	////System.out.println(word.indexOf("'"));
	//
	//System.out.println(wqord);
	//}

	positives = new HashSet<>();
	try {
	    FileReader fr = new FileReader("positive.txt");
	    BufferedReader br = new BufferedReader(fr);
	    String line = null;
	    while((line = br.readLine()) != null) {
		positives.add(line.replaceAll("\n", ""));
	    }
	    fr.close();
	}
	catch (Exception e) { e.printStackTrace(); }

	negatives = new HashSet<>();
	try {
	    FileReader fr = new FileReader("negative.txt");
	    BufferedReader br = new BufferedReader(fr);
	    String line = null;
	    while((line = br.readLine()) != null) {
		negatives.add(line.replaceAll("\n", ""));
	    }
	    fr.close();
	} catch (Exception e) { e.printStackTrace(); }


	File[] listOfFiles = (new File("input_dir")).listFiles();
	ArrayList<String> intermediateFiles = new ArrayList<>(listOfFiles.length);
	for(File file : listOfFiles) {
	    MapTask mapTask = new MapTask("input_dir/" + file.getName(), positives, negatives);
	    mapTask.countSentiment();
	    intermediateFiles.add(mapTask.getOutputFile());
	}
	SortTask sortTask = new SortTask(intermediateFiles);
	sortTask.sortFiles();
	String outputFile = sortTask.getOutputFile();
    }

}

