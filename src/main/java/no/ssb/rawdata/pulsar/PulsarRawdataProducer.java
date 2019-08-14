package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
    public String lastPosition() throws RawdataClosedException {
        try {
            try (Consumer<PulsarRawdataMessageContent> consumer = client.newConsumer(schema)
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .consumerName(producerName)
                    .subscriptionName("last-position-" + new Random().nextInt(Integer.MAX_VALUE))
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe()) {
                try {
                    MessageIdImpl lastMessageId = (MessageIdImpl) admin.topics().getLastMessageId(topic);
                    if (lastMessageId.getEntryId() == -1) {
                        return null; // topic is empty
                    }
                    consumer.seek(lastMessageId);
                    Message<PulsarRawdataMessageContent> message = consumer.receive(30, TimeUnit.SECONDS);
                    return message.getValue().position();
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(e);
                } finally {
                    consumer.unsubscribe();
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
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
    public void publish(String... positions) throws RawdataClosedException, RawdataContentNotBufferedException {
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataContentNotBufferedException(String.format("position %s is not in buffer", position));
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
                throw new RawdataContentNotBufferedException(String.format("position %s is not in buffer", position));
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
