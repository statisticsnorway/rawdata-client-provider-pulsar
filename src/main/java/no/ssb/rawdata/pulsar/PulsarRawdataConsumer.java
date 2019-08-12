package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PulsarRawdataConsumer implements RawdataConsumer {

    final Reader<PulsarRawdataMessageContent> reader;

    public PulsarRawdataConsumer(PulsarClient client, String topic, PulsarRawdataMessageId initialPosition) throws PulsarClientException {
        ReaderBuilder<PulsarRawdataMessageContent> builder = client.newReader(Schema.AVRO(PulsarRawdataMessageContent.class))
                .topic(topic);
        if (initialPosition == null) {
            builder.startMessageId(MessageId.earliest);
        } else {
            builder.startMessageId(initialPosition.messageId);
        }
        this.reader = builder.create();
    }

    @Override
    public String topic() {
        return reader.getTopic();
    }

    @Override
    public PulsarRawdataMessageContent receive(int timeout, TimeUnit unit) {
        try {
            Message<PulsarRawdataMessageContent> message = reader.readNext(timeout, unit);
            if (message == null) {
                return null;
            }
            return message.getValue();
        } catch (PulsarClientException e) {
            // TODO wrap consumer closed exception in a RawdataClosedException
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<PulsarRawdataMessageContent> receiveAsync() {
        return reader.readNextAsync().thenApply(m -> m.getValue());
    }

    @Override
    public String toString() {
        return "PulsarRawdataConsumer{" +
                "reader=" + reader +
                '}';
    }

    @Override
    public boolean isClosed() {
        return !reader.isConnected();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
