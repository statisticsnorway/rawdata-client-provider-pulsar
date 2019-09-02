package no.ssb.rawdata.pulsar;

import de.huxhorn.sulky.ulid.ULID;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class PulsarRawdataProducer implements RawdataProducer {

    final PulsarAdmin admin;
    final PulsarClient client;
    final String topic;
    final String producerName;
    final Schema<PulsarRawdataMessage> schema;

    final Producer<PulsarRawdataMessage> producer;
    final Map<String, PulsarRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> previousIdRef = new AtomicReference<>(ulid.nextValue());

    PulsarRawdataProducer(PulsarAdmin admin, PulsarClient client, String topic, String producerName, Schema<PulsarRawdataMessage> schema) throws PulsarClientException {
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
        return new PulsarRawdataMessage.Builder();
    }

    @Override
    public PulsarRawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        PulsarRawdataMessage.Builder builder = (PulsarRawdataMessage.Builder) _builder;
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }
        for (String position : positions) {
            PulsarRawdataMessage.Builder builder = buffer.remove(position);
            try {
                producer.newMessage()
                        .value(builder.ulid(getOrGenerateNextUlid(builder)).build())
                        .send();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ULID.Value getOrGenerateNextUlid(PulsarRawdataMessage.Builder builder) {
        ULID.Value id = builder.ulid;
        while (id == null) {
            ULID.Value previousUlid = previousIdRef.get();
            ULID.Value attemptedId = RawdataProducer.nextMonotonicUlid(this.ulid, previousUlid);
            if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                id = attemptedId;
            }
        }
        return id;
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
            PulsarRawdataMessage.Builder builder = buffer.remove(position);
            CompletableFuture<MessageId> future = producer.newMessage()
                    .value(builder.build())
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
