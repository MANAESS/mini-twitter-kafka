package com.manaess.kafka.tweets;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consommateur Kafka qui lit les tweets du topic "tweets-input",
 * extrait le champ JSON "message", puis parse chaque sous-champ
 * (user_id, timestamp, tweet_text, hashtags) par un simple split.
 *
 * @author Manal
 */
public class TweetConsumer {

    /** Nom du topic Kafka a consommer. */
    private static final String TOPIC = "tweets-input";

    /**
     * Point d'entree : configure le consommateur Kafka et boucle indefiniment
     * pour lire et afficher chaque nouveau tweet recu.
     *
     * @param args arguments de ligne de commande (non utilises ici)
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("En attente de tweets sur le topic '" + TOPIC + "'...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record.value());
                }
            }
        }
    }

    /**
     * Extrait le champ "message" du JSON recu, puis decoupe la chaine
     * pour en extraire et afficher user_id, timestamp, tweet_text et hashtags.
     *
     * @param jsonValue la valeur brute du message Kafka, au format JSON
     */
    private static void processRecord(String jsonValue) {
        JsonObject json = JsonParser.parseString(jsonValue).getAsJsonObject();
        String message = json.get("message").getAsString();

        String[] fields = message.split("; ");

        String userId = "";
        String timestamp = "";
        String tweetText = "";
        String hashtags = "";

        for (String field : fields) {
            String[] keyValue = field.split("=", 2);
            if (keyValue.length < 2) continue;

            String key = keyValue[0].trim();
            String value = keyValue[1].trim();

            switch (key) {
                case "user_id" -> userId = value;
                case "timestamp" -> timestamp = value;
                case "tweet_text" -> tweetText = value;
                case "hashtags" -> hashtags = value;
                default -> { /* champ inconnu, ignore */ }
            }
        }

        System.out.println("---- Nouveau tweet ----");
        System.out.println("user_id     : " + userId);
        System.out.println("timestamp   : " + timestamp);
        System.out.println("tweet_text  : " + tweetText);
        System.out.println("hashtags    : " + hashtags);
        System.out.println("------------------------");
    }
}