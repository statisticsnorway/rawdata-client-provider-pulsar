package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;

class PulsarRawdataProducer implements RawdataProducer {

    final PulsarClient client;
    final String topic;
    final String producerName;

    final Producer<PulsarRawdataPayload> producer;
    final Map<String, PulsarRawdataMessageContent> buffer = new ConcurrentHashMap<>();

    final Semaphore readSem = new Semaphore(0);
    final Semaphore writeSem = new Semaphore(0);
    final AtomicReference<Consumer<PulsarRawdataPayload>> lastExternalIdSubscriptionRef = new AtomicReference<>();
    final AtomicReference<Thread> lastMessageIdThreadRef = new AtomicReference<>();
    final AtomicReference<PulsarRawdataMessageId> lastMessageId = new AtomicReference<>();

    PulsarRawdataProducer(PulsarClient client, String topic, String producerName) throws PulsarClientException {
        this.client = client;
        this.topic = topic;
        this.producerName = producerName;
        try {
            lastExternalIdSubscriptionRef.set(client.newConsumer(Schema.AVRO(PulsarRawdataPayload.class))
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .consumerName(producerName)
                    .subscriptionName("last-external-id-tracking")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe());
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        lastMessageIdThreadRef.set(new Thread(new LastMessageIdRunnable(), topic + "::lastExternalIdTracking"));
        lastMessageIdThreadRef.get().start();
        producer = client.newProducer(Schema.AVRO(PulsarRawdataPayload.class))
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
        writeSem.release();
        try {
            readSem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return ofNullable(lastMessageId.get()).map(PulsarRawdataMessageId::getExternalId).orElse(null);
    }

    class LastMessageIdRunnable implements Runnable {
        @Override
        public void run() {
            try {
                Consumer<PulsarRawdataPayload> consumer = lastExternalIdSubscriptionRef.get();

                // drain all existing messages
                Message<PulsarRawdataPayload> prevMessage = null;
                Message<PulsarRawdataPayload> prevPrevMessage = null;
                for (; ; ) {
                    Message<PulsarRawdataPayload> message;
                    if (!((ConsumerImpl) consumer).hasMessageAvailable()) {
                        break;
                    }
                    message = consumer.receive(5, TimeUnit.MILLISECONDS);
                    if (message == null) {
                        break;
                    }
                    prevPrevMessage = prevMessage;
                    prevMessage = message;
                }
                if (prevPrevMessage != null) {
                    // acknowledge all except for the last message
                    consumer.acknowledgeCumulative(prevPrevMessage.getMessageId());
                }
                if (prevMessage != null) {
                    // set lastMessageId from the last message in the topic
                    lastMessageId.set(new PulsarRawdataMessageId(prevMessage.getMessageId(), prevMessage.getValue().getExternalId()));
                }

                for (; ; ) {
                    writeSem.acquire();
                    PulsarRawdataMessageId previousMessageId = null;
                    for (; ; ) {
                        if (!((ConsumerImpl) consumer).hasMessageAvailable()) {
                            break;
                        }
                        Message<PulsarRawdataPayload> message = consumer.receive(5, TimeUnit.SECONDS);
                        if (message == null) {
                            break;
                        }
                        previousMessageId = lastMessageId.getAndSet(new PulsarRawdataMessageId(message.getMessageId(), message.getValue().getExternalId()));
                    }
                    if (previousMessageId != null) {
                        consumer.acknowledgeCumulative(previousMessageId.messageId);
                    }
                    readSem.release();
                }
            } catch (PulsarClientException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        return new RawdataMessage.Builder() {
            String externalId;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessage.Builder position(String id) {
                this.externalId = id;
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public PulsarRawdataMessageContent build() {
                return new PulsarRawdataMessageContent(externalId, data);
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
        for (String externalId : positions) {
            if (!buffer.containsKey(externalId)) {
                throw new RawdataContentNotBufferedException(String.format("externalId %s is not in buffer", externalId));
            }
        }
        for (String externalId : positions) {
            PulsarRawdataPayload payload = buffer.remove(externalId).payload;
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
        for (String externalId : positions) {
            if (!buffer.containsKey(externalId)) {
                throw new RawdataContentNotBufferedException(String.format("externalId %s is not in buffer", externalId));
            }
        }
        List<CompletableFuture<MessageId>> result = new ArrayList<>();
        for (String externalId : positions) {
            PulsarRawdataPayload payload = buffer.remove(externalId).payload;
            CompletableFuture<MessageId> future = producer.newMessage()
                    .property("externalId", payload.externalId)
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
