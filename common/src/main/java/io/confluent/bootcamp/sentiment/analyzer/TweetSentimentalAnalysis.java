package io.confluent.bootcamp.sentiment.analyzer;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TweetSentimentalAnalysis {

    private final SentimentClassification sentimentClassification;
    StanfordCoreNLP coreNLP;

    public TweetSentimentalAnalysis() {
        Properties properties = new Properties();
        properties.put("annotators", "tokenize,ssplit,pos,parse,sentiment");
        coreNLP = new StanfordCoreNLP(properties);

        sentimentClassification = new SentimentClassification();
        sentimentClassification.setVeryPositive(2);
        sentimentClassification.setPositive(1);
        sentimentClassification.setNeutral(0);
        sentimentClassification.setNegative(-1);
        sentimentClassification.setVeryNegative(-2);
    }

    public double score (String text) {
        CoreDocument coreDocument = coreNLP.processToCoreDocument(String.valueOf(text));
        double score = 0;

        for (CoreSentence sentence : coreDocument.sentences()) {
            String sentimentClass = sentence.sentiment();
            score += sentimentClassification.computeScore(sentimentClass);
        }

        return score / coreDocument.sentences().size();
    }
}
