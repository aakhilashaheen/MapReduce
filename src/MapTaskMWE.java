import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;

public class MapTaskMWE {

    private static HashSet<String> positives, negatives;

    public MapTaskMWE() {
        positives = new HashSet<>();
        try {
            FileReader fr = new FileReader("data/positive.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                positives.add(line.replaceAll("\n", "").replaceAll("\r",""));
            }
            fr.close();
        } catch (Exception e) { e.printStackTrace(); }

        negatives = new HashSet<>();
        try {
            FileReader fr = new FileReader("data/negative.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                negatives.add(line.replaceAll("\n", "").replaceAll("\r",""));
            }
            fr.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public double countSentiment(String inputFile) {
        double pSentiment = 0, nSentiment = 0;
        try {
            FileReader fr = new FileReader(inputFile);
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                String[] words = line.replaceAll("--"," ").split("[^a-zA-Z\\-']+");
                for(int i = 0; i < words.length; ++i) {
                    String word = words[i].toLowerCase().replaceAll("[^a-zA-Z\\-']","");
                    if(word.contains("'")){
                        word = word.substring(0,word.indexOf("'"));
                    }
                    word.trim();
                    if(!word.equals("") && positives.contains(word))
                        pSentiment++;
                    else if(!word.equals("") &&negatives.contains(word))
                        nSentiment++;
                }
            }
            System.out.println("Positive: " + Double.toString(pSentiment) +
                    ", Negative: " + Double.toString(nSentiment) +
                    ", Total: " + Double.toString(pSentiment + nSentiment));
            br.close();
        } catch (Exception e) { }
        return (pSentiment - nSentiment)/(pSentiment + nSentiment);
    }

    public static void main(String[] args) {
        String inputFile = args[0];
        MapTaskMWE mwe = new MapTaskMWE();
        System.out.println("Sentiment = " + mwe.countSentiment(inputFile));
    }
}
