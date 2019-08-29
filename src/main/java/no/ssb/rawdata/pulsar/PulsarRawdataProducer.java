package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

class PulsarRawdataProducer implements RawdataProducer {

    final PulsarAdmin admin;
    final PulsarClient client;
    final String topic;
    final String producerName;
    final Schema<PulsarRawdataMessageContent> schema;

    final Producer<PulsarRawdataMessageContent> producer;
    final Map<String, PulsarRawdataMessageContent> buffer = new ConcurrentHashMap<>();

    PulsarRawdataProducer(PulsarAdmin admin, PulsarClient client, String topic, String producerName, Schema<PulsarRawdataMessageContent> schema) throws PulsarClientException {
        this.admin = admin;
        this.client = client;
        this.topic = topic;
        this.producerName = producerName;
        this.schema = schema;
        producer = client.newProducer(schema)
                .topic(topic)
                .producerName(producerName)
                .create();
    }

    @Override
    public String topic() {
        return producer.getTopic();
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        return new RawdataMessage.Builder() {
            String position;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessage.Builder position(String position) {
                this.position = position;
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public PulsarRawdataMessageContent build() {
                return new PulsarRawdataMessageContent(position, data);
            }
        };
    }

    @Override
    public PulsarRawdataMessageContent buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public PulsarRawdataMessageContent buffer(RawdataMessage message) throws RawdataClosedException {
        buffer.put(message.position(), (PulsarRawdataMessageContent) message);
        return (PulsarRawdataMessageContent) message;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }
        for (String position : positions) {
            PulsarRawdataMessageContent payload = buffer.remove(position);
            try {
                producer.newMessage()
                        .value(payload)
                        .send();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }
        List<CompletableFuture<MessageId>> result = new ArrayList<>();
        for (String position : positions) {
            PulsarRawdataMessageContent payload = buffer.remove(position);
            CompletableFuture<MessageId> future = producer.newMessage()
                    .value(payload)
                    .sendAsync();
            result.add(future);
        }
        return CompletableFuture.allOf(result.toArray(new CompletableFuture[result.size()]));
    }

    @Override
    public boolean isClosed() {
        return !producer.isConnected();
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
