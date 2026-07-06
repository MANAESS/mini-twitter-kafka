package com.heec.kafka.tweets;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Producteur Kafka qui genere 100 tweets aleatoires et les envoie
 * dans le topic "tweets-input".
 * <p>
 * Chaque message est encapsule dans un JSON avec une seule cle "message",
 * dont la valeur respecte le format :
 * {@code user_id=u123; timestamp=2025-05-22T10:00:00; tweet_text=Hello world! #java; hashtags=java}
 *
 * @author Manal
 */
public class TweetProducer {

    /** Nom du topic Kafka dans lequel les tweets sont envoyes. */
    private static final String TOPIC = "tweets-input";

    /** Generateur de nombres aleatoires, reutilise pour tous les tirages. */
    private static final Random RANDOM = new Random();

    /** Liste des identifiants d'utilisateurs fictifs utilises pour generer les tweets. */
    private static final List<String> USERS = List.of("u101", "u102", "u103", "u104", "u105");

    /** Liste des hashtags possibles pouvant apparaitre dans un tweet genere. */
    private static final List<String> HASHTAGS = List.of("java", "kafka", "bigdata", "spark", "cloud", "ia");

    /** Liste de phrases de base utilisees comme texte de depart d'un tweet. */
    private static final List<String> PHRASES = List.of(
            "Hello world!",
            "Apprentissage du streaming en temps reel",
            "Kafka c'est puissant",
            "Data engineering au top",
            "Traitement stateful vs stateless",
            "Un dimanche a coder"
    );

    /**
     * Point d'entree du programme : configure le producteur Kafka,
     * genere 100 tweets aleatoires et les envoie dans le topic "tweets-input".
     *
     * @param args arguments de ligne de commande (non utilises ici)
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                String tweetMessage = generateRandomTweet();

                JsonObject json = new JsonObject();
                json.addProperty("message", tweetMessage);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, null, json.toString());

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Tweet envoye -> partition=" + metadata.partition()
                                + " offset=" + metadata.offset());
                    }
                });
            }
            producer.flush();
            System.out.println("100 tweets envoyes dans le topic '" + TOPIC + "'.");
        }
    }

    /**
     * Genere une chaine de caracteres representant un tweet aleatoire,
     * au format : {@code user_id=...; timestamp=...; tweet_text=...; hashtags=...}
     *
     * @return la chaine de caracteres du tweet genere, prete a etre encapsulee en JSON
     */
    private static String generateRandomTweet() {
        String userId = USERS.get(RANDOM.nextInt(USERS.size()));
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        int nbHashtags = 1 + RANDOM.nextInt(2);
        StringBuilder hashtagsBuilder = new StringBuilder();
        StringBuilder tweetTextBuilder = new StringBuilder(PHRASES.get(RANDOM.nextInt(PHRASES.size())));

        for (int i = 0; i < nbHashtags; i++) {
            String tag = HASHTAGS.get(RANDOM.nextInt(HASHTAGS.size()));
            tweetTextBuilder.append(" #").append(tag);
            if (i > 0) hashtagsBuilder.append(",");
            hashtagsBuilder.append(tag);
        }

        return "user_id=" + userId
                + "; timestamp=" + timestamp
                + "; tweet_text=" + tweetTextBuilder
                + "; hashtags=" + hashtagsBuilder;
    }
}