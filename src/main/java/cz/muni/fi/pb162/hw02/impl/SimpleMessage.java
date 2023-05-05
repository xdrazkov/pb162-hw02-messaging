package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Map;
import java.util.Set;

public record SimpleMessage(Long id, Set<String> topics, Map<String, Object> data) implements Message {
}
