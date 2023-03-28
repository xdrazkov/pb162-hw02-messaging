package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerTest extends TestBase{
    private Broker broker;

    @BeforeEach
    public void setup() {
        broker = Messaging.broker();
    }

    @Test
    public void shouldBeEmpty() {
        assertThat(broker.listTopics()).isEmpty();
    }

    @Test
    public void shouldReturnMessageWhenStoring() {
        // given
        var topic = "house";
        Map<String, Object> data = Map.of("name", "Tom");
        // when
        var batch = push(msg(topic, data));
        var tom = last(batch, topic);
        // then
        assertThat(tom).isNotNull();
        softly.assertThat(tom.id()).describedAs("id of stored message").isNotNull();
        softly.assertThat(tom.topic()).describedAs("topic of stored message").isEqualTo(topic);
        softly.assertThat(tom.data()).describedAs("data of stored message").containsAllEntriesOf(data);
    }

    @Test
    public void shouldStoreMessagesInSingleTopic() {
        // given
        var stored = new HashSet<Message>();

        // when
        var polled = broker.poll(Map.of(), 1, List.of(TOPIC_HOUSE));
        var topics = broker.listTopics();
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from an empty broker")
                .isEmpty();
        softly.assertThat(topics)
                .describedAs("Topics listed from an empty broker")
                .isEmpty();

        // when
        var batch = push(msg(TOPIC_HOUSE, Map.of("name", "Tom")));
        var tom = last(batch, TOPIC_HOUSE);
        polled = broker.poll(Map.of(), 1, List.of(TOPIC_HOUSE));
        topics = broker.listTopics();
        stored.addAll(batch);
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from '{}' after pushing Tom", TOPIC_HOUSE)
                .containsExactlyInAnyOrderElementsOf(stored);
        softly.assertThat(topics).containsExactlyInAnyOrder(TOPIC_HOUSE);

        // when
        batch = push(msg(TOPIC_HOUSE, Map.of("name", "Jerry")));
        polled = broker.poll(Map.of(), 1, List.of(TOPIC_HOUSE));
        var jerry = last(batch, TOPIC_HOUSE);
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from '{}' after pushing Jerry", TOPIC_HOUSE)
                .containsExactlyInAnyOrder(tom);

        // when
        polled = broker.poll(Map.of(), 2, List.of());
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from no topics")
                .isEmpty();

        // when
        polled = broker.poll(Map.of(), 2, List.of(TOPIC_HOUSE));
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from topic '{}'", TOPIC_HOUSE)
                .containsExactlyInAnyOrder(tom, jerry);

        // when
        polled = broker.poll(Map.of(TOPIC_HOUSE, tom.id()), 1, List.of(TOPIC_HOUSE));
        // then
        softly.assertThat(polled)
                .describedAs("Message polled from topic with offset")
                .containsExactlyInAnyOrder(jerry);

        // when
        polled = broker.poll(Map.of(TOPIC_HOUSE, jerry.id()), 1, List.of(TOPIC_HOUSE));
        // then
        softly.assertThat(polled)
                .describedAs("Message polled from topic with maximal offset.")
                .isEmpty();

        // when
        polled = broker.poll(Map.of(TOPIC_HOUSE, jerry.id()), 2, List.of(TOPIC_HOUSE));
        // then
        softly.assertThat(polled)
                .describedAs("Messages polled from topic with maximal offset.")
                .isEmpty();
    }

    @Test
    public void shouldStoreMessagesInMultipleTopics() {
        // given
        var stored = new HashSet<Message>();

        // when
        var polled = broker.poll(Map.of(), 1, List.of(TOPIC_HOUSE, TOPIC_GARDEN));
        var topics = broker.listTopics();
        // then
        softly.assertThat(polled).isEmpty();
        softly.assertThat(topics).isEmpty();

        // when
        polled = broker.poll(Map.of(), 1, List.of(TOPIC_GARDEN));
        topics = broker.listTopics();
        // then
        softly.assertThat(polled).isEmpty();
        softly.assertThat(topics).isEmpty();

        // when
        var batch = push(
                msg(TOPIC_HOUSE, Map.of("name", "Tom")),
                msg(TOPIC_GARDEN, Map.of("name", "Jerry"))
        );
        polled = broker.poll(Map.of(), 2, List.of(TOPIC_HOUSE, TOPIC_GARDEN));
        topics = broker.listTopics();
        var tom = last(batch, TOPIC_HOUSE);
        var jerry = last(batch, TOPIC_GARDEN);
        stored.addAll(batch);
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(tom, jerry);
        softly.assertThat(topics).containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);

        // when
        polled = broker.poll(Map.of(), 1, List.of(TOPIC_HOUSE, TOPIC_GARDEN));
        topics = broker.listTopics();
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(tom, jerry);
        softly.assertThat(topics).containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);

        // when
        polled = broker.poll(Map.of(), 2, List.of(TOPIC_HOUSE));
        topics = broker.listTopics();
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(tom);
        softly.assertThat(topics).containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);


        // when
        polled = broker.poll(Map.of(), 2, List.of(TOPIC_GARDEN));
        topics = broker.listTopics();
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(jerry);
        softly.assertThat(topics).containsExactlyInAnyOrder(TOPIC_HOUSE, TOPIC_GARDEN);

        // when
        batch = push(msg(TOPIC_GARDEN, Map.of("name", "Nibbles")));
        polled = broker.poll(Map.of(), 2, List.of(TOPIC_HOUSE, TOPIC_GARDEN));
        stored.addAll(batch);
        var nibbles = last(batch, TOPIC_GARDEN);
        // then
        softly.assertThat(batch).hasSize(1);
        softly.assertThat(polled).containsExactlyInAnyOrder(tom, jerry, nibbles);


        // when
        polled = broker.poll(Map.of(TOPIC_GARDEN, jerry.id()), 1, List.of(TOPIC_GARDEN));
        // then
        softly.assertThat(nibbles.data()).containsEntry("name", "Nibbles");
        softly.assertThat(polled).containsExactlyInAnyOrder(nibbles);

        // when
        polled = broker.poll(Map.of(TOPIC_GARDEN, jerry.id()), 2, List.of(TOPIC_GARDEN));
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(nibbles);

        // when
        polled = broker.poll(Map.of(TOPIC_GARDEN, nibbles.id()), 2, List.of(TOPIC_GARDEN));
        // then
        softly.assertThat(polled).isEmpty();

        // when
        polled = broker.poll(Map.of(TOPIC_GARDEN, jerry.id(), TOPIC_HOUSE, tom.id()), 2, List.of(TOPIC_HOUSE, TOPIC_GARDEN));
        // then
        softly.assertThat(polled).containsExactlyInAnyOrder(nibbles);
    }

    private Collection<Message> push(Message... messages) {
        return broker.push(Arrays.asList(messages));
    }
}
