package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IntegrationTest extends TestBase {

    private Broker broker;
    private Producer producer;
    private Consumer consumer;

    @BeforeEach
    public void setup() {
        broker = Messaging.broker();
        producer = Messaging.producer(broker);
        consumer = Messaging.consumer(broker);

        // Populate broker with some data
        broker.push(List.of(
                msg(TOPIC_HOUSE, Map.of("name", "Tom")),
                msg(TOPIC_HOUSE, Map.of("name", "Jerry")),
                msg(TOPIC_HOUSE, Map.of("name", "Butch")),
                msg(TOPIC_GARDEN, Map.of("name", "Nibbles"))
        ));
    }

    @Test
    public void shouldListInitialTopics() {
        // then
        softly.assertThat(broker.listTopics())
                .describedAs("Topics from broker")
                .containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);
        softly.assertThat(consumer.listTopics())
                .describedAs("Topics from consumer")
                .containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);
        softly.assertThat(producer.listTopics())
                .describedAs("Topics from producer")
                .containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);
    }

    @Test
    public void shouldConsumeAllData() {
        // when
        var consumed = consumer.consume(2, TOPIC_HOUSE, TOPIC_GARDEN);
        var ids = consumed.stream().map(Message::id).collect(Collectors.toSet());
        var topics = consumed.stream().map(Message::topic).toList();
        var names = consumed.stream()
                .map(Message::data).map(d -> d.get("name").toString()).collect(Collectors.toSet());
        // then
        softly.assertThat(consumed)
                .describedAs("Consume two messages from each topic")
                .hasSize(3);
        softly.assertThat(ids)
                .describedAs("Unique IDs of consumed messages")
                .hasSameSizeAs(consumed);
        softly.assertThat(topics)
                .describedAs("Topics of consumed messages")
                .containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_HOUSE, TOPIC_GARDEN);
        softly.assertThat(names)
                .describedAs("Names extracted from consumed messages")
                .hasSameSizeAs(consumed)
                .contains("Nibbles")
                .areAtLeast(2, isOneOf("Tom", "Jerry", "Butch"));

    }

    private Condition<String> isOneOf(String... names) {
        return new Condition<>(s -> Arrays.asList(names).contains(s), "Has nam from '{}' topic", TOPIC_HOUSE);
    }

}
