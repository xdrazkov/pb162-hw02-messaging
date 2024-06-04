package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;

import java.util.Collection;
import java.util.Collections;

public class SimpleProducer extends SimpleClient implements Producer {
    /**
     * Constructor
     *
     * @param broker the broker
     */
    public SimpleProducer(Broker broker) {
        super(broker);
    }

    @Override
    public Message produce(Message message) {
        return produce(Collections.singletonList(message)).iterator().next();
    }

    @Override
    public Collection<Message> produce(Collection<Message> messages) {
        return getBroker().push(messages);
    }
}
