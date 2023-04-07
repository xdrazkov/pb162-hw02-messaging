package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MyConsumer implements Consumer {
    private final Broker broker;
    private Map<String, Long> offsets = new HashMap<>();

    /**
     * Constructor
     * @param broker the broker
     */
    public MyConsumer(Broker broker) {
        this.broker = broker;
    }

    @Override
    public Broker broker() {
        return broker;
    }

    @Override
    public Collection<String> listTopics() {
        return broker.listTopics();
    }

    @Override
    public Collection<Message> consume(int num, String... topics) {
        Collection<Message> messages = broker.poll(offsets, num, List.of(topics));
        List<Message> sortedMessages = new ArrayList<>(messages);
        sortedMessages.sort(Comparator.comparingLong(Message::id));

        LinkedHashMap<String, Integer> topicsAdded = new LinkedHashMap<>();
        for (String topic : topics) {
            topicsAdded.put(topic, 0);
        }

        for (Message message : sortedMessages) {
            for (String topic : message.topics()) {
                if (!topicsAdded.containsKey(topic) || topicsAdded.get(topic) == num) {
                    continue;
                }
                long offsetValue = offsets.get(topic) == null ? 0L : offsets.get(topic);
                if (message.id() > offsetValue) {
                    offsets.put(topic, message.id());
                    topicsAdded.put(topic, topicsAdded.get(topic) + 1);
                    break;
                }
            }
        }
        return messages;
    }

    @Override
    public Collection<Message> consume(Map<String, Long> offsets, int num, String... topics) {
        return broker.poll(offsets, num, List.of(topics));
    }

    @Override
    public Map<String, Long> getOffsets() {
        return new HashMap<>(offsets);
    }

    @Override
    public void setOffsets(Map<String, Long> offsets) {
        this.offsets = offsets;
    }

    @Override
    public void clearOffsets() {
        this.offsets = new HashMap<>();
    }

    @Override
    public void updateOffsets(Map<String, Long> offsets) {
        this.offsets.putAll(offsets);
    }
}
