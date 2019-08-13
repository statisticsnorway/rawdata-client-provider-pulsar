package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataConsumer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PulsarRawdataConsumer implements RawdataConsumer {

    final Consumer<PulsarRawdataMessageContent> consumer;

    public PulsarRawdataConsumer(PulsarClient client, String topic, PulsarRawdataMessageId initialPosition, Schema<PulsarRawdataMessageContent> schema) throws PulsarClientException {
        // TODO Go back to using reader once bug has been fixed: https://github.com/apache/pulsar/issues/4941
        this.consumer = client.newConsumer(schema)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("rawdata-m" + new Random().nextInt(Integer.MAX_VALUE))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .acknowledgmentGroupTime(5, TimeUnit.SECONDS)
                .subscribe();
        if (initialPosition != null) {
            consumer.seek(initialPosition.messageId);
            Message<PulsarRawdataMessageContent> message = consumer.receive(30, TimeUnit.SECONDS);
            if (message == null) {
                throw new RuntimeException("Unable to find message that seek was set to");
            }
            consumer.acknowledge(message.getMessageId()); // auto-acknowledge
        }
    }

    @Override
    public String topic() {
        return consumer.getTopic();
    }

    @Override
    public PulsarRawdataMessageContent receive(int timeout, TimeUnit unit) {
        try {
            Message<PulsarRawdataMessageContent> message = consumer.receive(timeout, unit);
            if (message == null) {
                return null;
            }
            consumer.acknowledge(message.getMessageId()); // auto-acknowledge
            return message.getValue();
        } catch (PulsarClientException e) {
            // TODO wrap consumer closed exception in a RawdataClosedException
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<PulsarRawdataMessageContent> receiveAsync() {
        return consumer.receiveAsync().thenApply(m -> {
            try {
                consumer.acknowledge(m.getMessageId()); // auto-acknowledge
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
            return m.getValue();
        });
    }

    @Override
    public String toString() {
        return "PulsarRawdataConsumer{" +
                "consumer=" + consumer +
                '}';
    }

    @Override
    public boolean isClosed() {
        return !consumer.isConnected();
    }

    @Override
    public void close() {
        try {
            consumer.unsubscribe();
            consumer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
