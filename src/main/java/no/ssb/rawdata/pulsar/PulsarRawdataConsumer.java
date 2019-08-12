package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessageId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PulsarRawdataConsumer implements RawdataConsumer {

    final Consumer<PulsarRawdataPayload> consumer;

    PulsarRawdataConsumer(PulsarAdmin admin, PulsarClient client, String namespace, String topic, String consumerName, String subscription) throws PulsarClientException, PulsarAdminException {
        List<String> topics = admin.namespaces().getTopics(namespace);
        boolean startFromEarliestMessage = true;
        if (topics.contains(topic)) {
            List<String> subscriptions = admin.topics().getSubscriptions(topic);
            startFromEarliestMessage = !subscriptions.contains(subscription);
        }

        ConsumerBuilder<PulsarRawdataPayload> builder = client.newConsumer(Schema.AVRO(PulsarRawdataPayload.class))
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .consumerName(consumerName)
                .subscriptionName(subscription);
        if (startFromEarliestMessage) {
            builder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        }
        this.consumer = builder.subscribe();
    }

    @Override
    public String topic() {
        return consumer.getTopic();
    }

    @Override
    public String subscription() {
        return consumer.getSubscription();
    }

    @Override
    public PulsarRawdataMessage receive(int timeout, TimeUnit unit) {
        try {
            Message<PulsarRawdataPayload> message = consumer.receive(timeout, unit);
            if (message == null) {
                return null;
            }
            return toPulsarRawdataMessage(message);
        } catch (PulsarClientException e) {
            // TODO wrap consumer closed exception in a RawdataClosedException
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<PulsarRawdataMessage> receiveAsync() {
        return consumer.receiveAsync().thenApply(m -> toPulsarRawdataMessage(m));
    }

    PulsarRawdataMessage toPulsarRawdataMessage(Message<PulsarRawdataPayload> message) {
        return new PulsarRawdataMessage(new PulsarRawdataMessageId(message.getMessageId(), message.getValue().getExternalId()), new PulsarRawdataMessageContent(message.getValue()));
    }

    @Override
    public void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException {
        try {
            consumer.acknowledge(((PulsarRawdataMessageId) id).messageId);
        } catch (PulsarClientException e) {
            // TODO wrap consumer closed exception in a RawdataClosedException
            throw new RuntimeException(e);
        }
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
            consumer.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
