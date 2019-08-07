package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class PulsarRawdataClient implements RawdataClient {

    final PulsarClient client;
    final String tenant;
    final String namespace;
    final String producerName;
    final String consumerName;

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PulsarRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PulsarRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    PulsarRawdataClient(PulsarClient client, String tenant, String namespace, String producerName, String consumerName) {
        this.client = client;
        this.tenant = tenant;
        this.namespace = namespace;
        this.producerName = producerName;
        this.consumerName = consumerName;
    }

    @Override
    public PulsarRawdataProducer producer(String topicName) {
        PulsarRawdataProducer producer;
        try {
            producer = new PulsarRawdataProducer(client, toQualifiedPulsarTopic(topicName), producerName);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        this.producers.add(producer);
        return producer;
    }

    String toQualifiedPulsarTopic(String topicName) {
        return "persistent://" + tenant + "/" + namespace + "/" + topicName;
    }

    @Override
    public PulsarRawdataConsumer consumer(String topicName, String subscription) {
        PulsarRawdataConsumer consumer;
        try {
            consumer = new PulsarRawdataConsumer(client, toQualifiedPulsarTopic(topicName), consumerName, subscription);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        for (PulsarRawdataProducer producer : producers) {
            try {
                producer.close();
            } catch (Throwable t) {
                // ignore
            }
        }
        producers.clear();
        for (PulsarRawdataConsumer consumer : consumers) {
            try {
                consumer.close();
            } catch (Throwable t) {
                // ignore
            }
        }
        consumers.clear();
        try {
            client.close();
        } catch (PulsarClientException e) {
            // ignore
            try {
                client.shutdown();
            } catch (PulsarClientException ex) {
                // ignore
            }
        }
        closed.set(true);
    }
}
