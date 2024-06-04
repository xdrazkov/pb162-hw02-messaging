package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumer extends SimpleClient implements Consumer {
    private Map<String, Long> offsets = new HashMap<>();

    /**
     * Constructor
     * @param broker the broker
     */
    public MessageConsumer(Broker broker) {
        super(broker);
    }

    @Override
    public Collection<Message> consume(int num, String... topics) {
        Collection<Message> messages = getBroker().poll(offsets, num, List.of(topics));
        List<Message> sortedMessages = new ArrayList<>(messages);
        sortedMessages.sort(Comparator.comparingLong(Message::id));

        HashMap<String, Integer> topicsAdded = new HashMap<>();
        for (String topic : topics) {
            topicsAdded.put(topic, 0);
        }

        for (Message message : sortedMessages) {
            for (String topic : message.topics()) {
                if (!topicsAdded.containsKey(topic) || topicsAdded.get(topic) == num) {
                    continue;
                }
                long offsetValue = offsets.getOrDefault(topic, 0L);
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
        return getBroker().poll(offsets, num, List.of(topics));
    }

    @Override
    public Map<String, Long> getOffsets() {
        return new HashMap<>(offsets);
    }

    @Override
    public void setOffsets(Map<String, Long> offsets) {
        this.offsets = new HashMap<>(offsets);
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
