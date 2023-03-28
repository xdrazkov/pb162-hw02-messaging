package cz.muni.fi.pb162.hw02.mesaging.broker;

import java.util.Collection;
import java.util.Map;

/**
 * This class represents a messaging broker. A database of sorts used to store and deliver messages.
 * The broker stores messages in topics (retrievable from message object via {@link Message#topic()})
 *
 * In real world, this is the part of the system which would be running as a server.
 * Clients would then connect to the broker over the network.
 * For our purpose the broker will be implemented as part of the same codebase.
 */
public interface Broker {

    /**
     * Lists topics currently registered by the broker.
     * A registered topic is any topic which was previously used to store data
     *
     * @return list of available topics
     */
    Collection<String> listTopics();


    /**
     * Store messages to their destinations (as defined by {@link Message#topic()}
     *
     * @param messages messages to store
     * @return Equivalent message objects with populated id fields
     */
    Collection<Message> push(Collection<Message> messages);

    /**
     * Polls requested number of unread messages from this broker's topics.
     * <br>
     *
     * Message is considered read if its {@code id} is lower or equal to the last read id for the particular topic
     * in offset definition. If no such entry exists,then all messages from that topic are considered unread.
     * <br>
     *
     * The offset definition is a map where
     *      - the key is a topic name
     *      - the value is an id of the last message read in that topic
     *
     * @param offsets offset definition
     * @param topics list of topics
     * @return collection of messages
     */
    Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics);
}
