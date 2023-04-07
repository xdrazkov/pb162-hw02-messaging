package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MyBroker implements Broker {
    private final HashMap<String, ArrayList<Message>> topics = new HashMap<>();
    private static Long messageID = 0L;

    @Override
    public Collection<String> listTopics() {
        return topics.keySet();
    }

    @Override
    public Collection<Message> push(Collection<Message> messages) {
        Collection<Message> returnValue = new ArrayList<>();
        for (Message message : messages) {
            Message newMessage = new MyMessage(getNewMessageID(), message.topics(), message.data());
            returnValue.add(newMessage);
            for (String topic : message.topics()) {
                if (!topics.containsKey(topic)) {
                    topics.put(topic, new ArrayList<>());
                }
                topics.get(topic).add(newMessage);
            }
        }
        return returnValue;
    }

    @Override
    public Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics) {
        Set<Message> messages = new HashSet<>();
        for (String topic : topics) {
            int messagesAdded = 0;
            if (!this.topics.containsKey(topic)) {
                continue;
            }
            for (Message message : this.topics.get(topic)) {
                long topicOffset = offsets.getOrDefault(topic, 0L);
                if (message.id() > topicOffset && message.topics().contains(topic)) {
                    messages.add(message);
                    if (++messagesAdded == num) {
                        break;
                    }
                }
            }
        }
        return messages;
    }

    /**
     * Returns new message ID
     * @return new message id
     */
    private Long getNewMessageID() {
        messageID++;
        return messageID;
    }
}
