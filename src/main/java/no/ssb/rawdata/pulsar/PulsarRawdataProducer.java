package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class PulsarRawdataProducer implements RawdataProducer {

    final PulsarClient client;
    final String topic;
    final String producerName;

    final Producer<PulsarRawdataPayload> producer;
    final Map<String, PulsarRawdataMessageContent> buffer = new ConcurrentHashMap<>();

    //final Reader<PulsarRawdataPayload> reader;

    PulsarRawdataProducer(PulsarClient client, String topic, String producerName) throws PulsarClientException {
        this.client = client;
        this.topic = topic;
        this.producerName = producerName;
        /*
        try {
            reader = client.newReader(JSONSchema.of(PulsarRawdataPayload.class))
                    .topic(topic)
                    .readerName(producerName + "-lastMessageIdReader")
                    .receiverQueueSize(1)
                    .startMessageIdInclusive()
                    .startMessageId(MessageId.latest)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
         */
        producer = client.newProducer(JSONSchema.of(PulsarRawdataPayload.class))
                .topic(topic)
                .producerName(producerName)
                .create();
    }

    @Override
    public String topic() {
        return producer.getTopic();
    }

    @Override
    public String lastExternalId() throws RawdataClosedException {
        try {
            Reader<PulsarRawdataPayload> reader = client.newReader(JSONSchema.of(PulsarRawdataPayload.class))
                    .topic(topic)
                    .readerName(producerName + "-lastMessageIdReader")
                    .startMessageId(MessageId.latest)
                    .startMessageIdInclusive()
                    .create();
            if (!reader.hasMessageAvailable()) {
                return null;
            }
            Message<PulsarRawdataPayload> lastMessage = reader.readNext(3, TimeUnit.SECONDS);
            if (lastMessage == null) {
                throw new IllegalStateException("This must be a bug in hasMessageAvailable or readNext method!");
            }
            PulsarRawdataPayload payload = lastMessage.getValue();
            return payload.getExternalId();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RawdataMessageContent.Builder builder() throws RawdataClosedException {
        return new RawdataMessageContent.Builder() {
            String externalId;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessageContent.Builder externalId(String externalId) {
                this.externalId = externalId;
                return this;
            }

            @Override
            public RawdataMessageContent.Builder put(String key, byte[] payload) {
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
    public PulsarRawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public PulsarRawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException {
        buffer.put(content.externalId(), (PulsarRawdataMessageContent) content);
        return (PulsarRawdataMessageContent) content;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        return publish(externalIds.toArray(new String[externalIds.size()]));
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        try {
            List<CompletableFuture<? extends RawdataMessageId>> futures = publishAsync(externalIds);
            List<PulsarRawdataMessageId> result = new ArrayList<>();
            for (CompletableFuture<? extends RawdataMessageId> future : futures) {
                result.add((PulsarRawdataMessageId) future.join());
            }
            return result;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RawdataContentNotBufferedException) {
                throw (RawdataContentNotBufferedException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    @Override
    public List<CompletableFuture<? extends RawdataMessageId>> publishAsync(String... externalIds) {
        for (String externalId : externalIds) {
            if (!buffer.containsKey(externalId)) {
                throw new RawdataContentNotBufferedException(String.format("externalId %s is not in buffer", externalId));
            }
        }
        List<CompletableFuture<? extends RawdataMessageId>> result = new ArrayList<>();
        for (String externalId : externalIds) {
            PulsarRawdataPayload payload = buffer.remove(externalId).payload;
            CompletableFuture<PulsarRawdataMessageId> future = producer.newMessage()
                    .property("externalId", payload.externalId)
                    .value(payload)
                    .sendAsync()
                    .thenApply(messageId -> new PulsarRawdataMessageId(messageId, externalId));
            result.add(future);
        }
        return result;
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
