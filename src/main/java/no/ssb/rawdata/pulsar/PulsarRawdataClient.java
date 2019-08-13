package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class PulsarRawdataClient implements RawdataClient {

    static final Schema<PulsarRawdataMessageContent> schema = Schema.AVRO(PulsarRawdataMessageContent.class);

    final PulsarAdmin admin;
    final PulsarClient client;
    final String tenant;
    final String namespace;
    final String producerName;

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PulsarRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PulsarRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    final AtomicReference<Driver> prestoDriver = new AtomicReference<>();

    PulsarRawdataClient(PulsarAdmin admin, PulsarClient client, String tenant, String namespace, String producerName) {
        this.admin = admin;
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
            PulsarRawdataMessageId initialMessage = null;
            if (initialPosition != null) {
                initialMessage = findMessageId(topic, initialPosition);
            }
            consumer = new PulsarRawdataConsumer(client, toQualifiedPulsarTopic(topic), initialMessage, schema);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        consumers.add(consumer);
        return consumer;
    }

    PulsarRawdataMessageId findMessageId(String topic, String position) {
        String url = "jdbc:presto://localhost:8081/pulsar";
        if (prestoDriver.get() == null) {
            try {
                prestoDriver.set(DriverManager.getDriver(url));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        try (Connection connection = prestoDriver.get().connect(url, properties)) {
            PreparedStatement ps = connection.prepareStatement(String.format("SELECT __message_id__ FROM pulsar.\"%s/%s\".\"%s\" WHERE position = ?", tenant, namespace, topic));
            ps.setString(1, position);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String serializedMessageId = rs.getString(1);
                Pattern pattern = Pattern.compile("\\((?<ledgerId>[0-9]+),(?<entryId>[0-9]+),(?<batchIndex>[0-9]+)\\)");
                Matcher m = pattern.matcher(serializedMessageId);
                m.matches();
                long ledgerId = Long.parseLong(m.group("ledgerId"));
                long entryId = Long.parseLong(m.group("entryId"));
                int batchIndex = Integer.parseInt(m.group("batchIndex"));
                MessageId id = new BatchMessageIdImpl(ledgerId, entryId, -1, batchIndex);
                return new PulsarRawdataMessageId(id, position);
            }
            /*
             * Not found using Pulsar SQL. Check to see if this could be the very last message in the topic, which is
             * not visible to Pulsar SQL due to an open bug: https://github.com/apache/pulsar/issues/3828
             */
            try (Consumer<PulsarRawdataMessageContent> consumer = client.newConsumer(schema)
                    .topic(toQualifiedPulsarTopic(topic))
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("rawdata-findMessageId-" + new Random().nextInt(Integer.MAX_VALUE))
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .subscribe()) {
                try {
                    consumer.seek(admin.topics().getLastMessageId(toQualifiedPulsarTopic(topic)));
                    Message<PulsarRawdataMessageContent> message = consumer.receive(3, TimeUnit.SECONDS);
                    if (message == null) {
                        return null; // not found
                    }
                    return new PulsarRawdataMessageId(message.getMessageId(), position); // found as last message in topic
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(e);
                } finally {
                    consumer.unsubscribe();
                }
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
        admin.close();
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
