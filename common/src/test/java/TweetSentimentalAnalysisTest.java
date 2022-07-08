import io.confluent.bootcamp.sentiment.analyzer.TweetSentimentalAnalysis;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TweetSentimentalAnalysisTest {

    TweetSentimentalAnalysis analysis = new TweetSentimentalAnalysis();

    @Test
    public void simpleTest() {
        double value = analysis.score("Whatever. Not interested");

        assertEquals(value, 0.0);
    }

    @Test
    public void excitedTest() {
        double value = analysis.score("Amazing! Awesome!");

        assertEquals(value, 2.0);
    }

    @Test
    public void sosoTest() {
        double value = analysis.score("It is okay");

        assertEquals(value, 1.0);
    }

    @Test
    public void mixedTest() {
        double value = analysis.score("It is okay. Amazing! Awesome!");

        assertEquals(value, 1.0);
    }

}
