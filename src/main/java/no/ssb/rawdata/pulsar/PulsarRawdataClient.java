package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class PulsarRawdataClient implements RawdataClient {

    static final Schema<PulsarRawdataMessageContent> schema = Schema.AVRO(PulsarRawdataMessageContent.class);

    final PulsarClient client;
    final String tenant;
    final String namespace;
    final String producerName;

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PulsarRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PulsarRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    final AtomicReference<Driver> prestoDriver = new AtomicReference<>();

    PulsarRawdataClient(PulsarClient client, String tenant, String namespace, String producerName) {
        this.client = client;
        this.tenant = tenant;
        this.namespace = namespace;
        this.producerName = producerName;
    }

    @Override
    public PulsarRawdataProducer producer(String topicName) {
        PulsarRawdataProducer producer;
        try {
            producer = new PulsarRawdataProducer(client, toQualifiedPulsarTopic(topicName), producerName, schema);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, String initialPosition) {
        PulsarRawdataConsumer consumer;
        try {
            //PulsarRawdataMessageId initialMessage = findMessageId(topic, initialPosition); // TODO
            consumer = new PulsarRawdataConsumer(client, toQualifiedPulsarTopic(topic), null, schema);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        consumers.add(consumer);
        return consumer;
    }

    public PulsarRawdataMessageId findMessageId(String topic, String position) {
        String url = "jdbc:presto://localhost:8081/pulsar";
        if (prestoDriver.get() == null) {
            try {
                prestoDriver.set(DriverManager.getDriver(url));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        Properties properties = new Properties();
        //properties.setProperty("user", "test");
        //properties.setProperty("password", "secret");
        //properties.setProperty("SSL", "true");
        try (Connection connection = prestoDriver.get().connect(url, properties)) {
            PreparedStatement ps = connection.prepareStatement(String.format("SELECT __message_id__ FROM pulsar.\"%s/%s\".\"%s\" WHERE position = ?", tenant, namespace, topic));
            ps.setString(1, position);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println("SUCCESS!!!!");
            }
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    String toQualifiedPulsarTopic(String topicName) {
        return "persistent://" + tenant + "/" + namespace + "/" + topicName;
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
