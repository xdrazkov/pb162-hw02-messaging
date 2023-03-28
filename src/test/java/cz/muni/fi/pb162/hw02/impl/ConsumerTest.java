package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ConsumerTest extends TestBase {



    private TestBroker broker;
    private Consumer consumer;


    @BeforeEach
    public void setup() {
        broker = new TestBroker();
        consumer = Messaging.consumer(broker);
    }

    @Test
    public void shouldReturnTopics() {
        // then
        softly.assertThat(consumer.listTopics()).isEmpty();


        // when
        broker.setTopics("house", "garden");
        // then
        softly.assertThat(consumer.listTopics())
                .describedAs("Topics returned by producer")
                .containsExactlyInAnyOrder("house", "garden");

        // when
        broker.setTopics("pool");
        // then
        softly.assertThat(consumer.listTopics())
                .describedAs("Topics returned by producer")
                .containsExactlyInAnyOrder("pool");
    }


    @Test
    public void shouldConsumeNothingFromEmptyBroker() {
        // when
        var consumed = consumer.consume(3, "house");
        // when
        softly.assertThat(consumed)
                .describedAs("Messages from empty broker")
                .isEmpty();
    }

    @Test
    public void shouldHaveCorrectOffset() {
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offset before consuming any messages")
                .isEmpty();;

        // when
        broker.setBatch(
                msg(1L, TOPIC_HOUSE, Map.of()),
                msg(10L, TOPIC_GARDEN, Map.of())
        );
        consumer.consume(1, TOPIC_HOUSE, TOPIC_GARDEN);
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after consuming single message from each topic")
                .containsExactlyInAnyOrderEntriesOf(Map.of(TOPIC_HOUSE, 1L, TOPIC_GARDEN, 10L));

        // when
        broker.setBatch(
                msg(2L, TOPIC_HOUSE, Map.of())
        );
        consumer.consume(1, TOPIC_HOUSE);
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after consuming another message from each topic")
                .containsExactlyInAnyOrderEntriesOf(Map.of(TOPIC_HOUSE, 2L, TOPIC_GARDEN, 10L));

        // when
        consumer.updateOffsets(Map.of(TOPIC_HOUSE, 1L));
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after updating offset for topic '" + TOPIC_HOUSE + "'")
                .containsExactlyInAnyOrderEntriesOf(Map.of(TOPIC_HOUSE, 1L, TOPIC_GARDEN, 10L));

        // when
        consumer.setOffsets(Map.of(TOPIC_HOUSE, 2L));
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after setting custom offset")
                .containsExactlyInAnyOrderEntriesOf(Map.of(TOPIC_HOUSE, 2L));

        // when
        consumer.clearOffsets();
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after clear")
                .isEmpty();

        // when
        broker.setBatch(
                msg(1L, TOPIC_HOUSE, Map.of()),
                msg(2L, TOPIC_HOUSE, Map.of()),
                msg(10L, TOPIC_GARDEN, Map.of())
        );
        consumer.consume(2, TOPIC_HOUSE, TOPIC_GARDEN);
        // then
        softly.assertThat(consumer.getOffsets())
                .describedAs("Consumer offsets after consuming two messages for each topic")
                .containsExactlyInAnyOrderEntriesOf(Map.of(TOPIC_HOUSE, 2L, TOPIC_GARDEN, 10L));
    }
}
