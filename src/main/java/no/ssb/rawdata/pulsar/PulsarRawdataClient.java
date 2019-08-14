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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger log = LoggerFactory.getLogger(PulsarRawdataClient.class);

    static final Schema<PulsarRawdataMessageContent> schema = Schema.AVRO(PulsarRawdataMessageContent.class);
    static final AtomicReference<Driver> prestoDriver = new AtomicReference<>();

    final PulsarAdmin admin;
    final PulsarClient client;
    final String prestoUrl;
    final Properties prestoJdbcProperties;
    final String tenant;
    final String namespace;
    final String producerName;

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PulsarRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PulsarRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    PulsarRawdataClient(PulsarAdmin admin, PulsarClient client, String prestoUrl, Properties prestoJdbcProperties, String tenant, String namespace, String producerName) {
        this.admin = admin;
        this.client = client;
        this.prestoUrl = prestoUrl;
        this.prestoJdbcProperties = prestoJdbcProperties;
        this.tenant = tenant;
        this.namespace = namespace;
        this.producerName = producerName;
    }

    @Override
    public PulsarRawdataProducer producer(String topicName) {
        PulsarRawdataProducer producer;
        try {
            producer = new PulsarRawdataProducer(admin, client, toQualifiedPulsarTopic(topicName), producerName, schema);
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
        /*
         * Check first to see if this could be the very last message in the topic, which is not visible to Pulsar SQL
         * due to an open bug: https://github.com/apache/pulsar/issues/3828
         */
        PulsarRawdataMessageId lastMessageId = getLastMessageIdUsingAdminClientAndConsumerSeek(toQualifiedPulsarTopic(topic));
        if (lastMessageId != null && position.equals(lastMessageId.getPosition())) {
            return lastMessageId; // last message matches, no need to run Pulsar SQL
        }

        if (prestoUrl != null) {
            try {
                return getIdOfPositionUsingPulsarSQL(topic, position);
            } catch (Error | RuntimeException e) {
                String msg = String.format("Error while attempting to use Pulsar SQL to get the message-id of message on topic '%s' with position '%s'. Falling back to full topic scan", topic, position);
                if (log.isDebugEnabled()) {
                    log.debug(msg, e);
                } else {
                    log.info(msg);
                }
            }
        }

        return fullTopicScanForMessageId(toQualifiedPulsarTopic(topic), position);
    }

    private PulsarRawdataMessageId getLastMessageIdUsingAdminClientAndConsumerSeek(String topic) {
        try (Consumer<PulsarRawdataMessageContent> consumer = client.newConsumer(schema)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("get-last-message-" + new Random().nextInt(Integer.MAX_VALUE))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe()) {
            try {
                MessageIdImpl lastMessageId = (MessageIdImpl) admin.topics().getLastMessageId(topic);
                if (lastMessageId.getEntryId() == -1) {
                    return null; // topic is empty
                }
                consumer.seek(lastMessageId);
                Message<PulsarRawdataMessageContent> message = consumer.receive(3, TimeUnit.SECONDS);
                if (message == null) {
                    return null; // not found
                }
                PulsarRawdataMessageContent value = message.getValue();
                return new PulsarRawdataMessageId(message.getMessageId(), value.position()); // found as last message in topic
            } catch (PulsarAdminException e) {
                throw new RuntimeException(e);
            } finally {
                consumer.unsubscribe();
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private PulsarRawdataMessageId fullTopicScanForMessageId(String topic, String position) {
        try (Consumer<PulsarRawdataMessageContent> consumer = client.newConsumer(schema)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("scan-position-" + new Random().nextInt(Integer.MAX_VALUE))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            try {
                int i = 0;
                Message<PulsarRawdataMessageContent> message;
                MessageId lastMessageId = admin.topics().getLastMessageId(topic);
                while ((message = consumer.receive(30, TimeUnit.SECONDS)) != null) {
                    if (position.equals(message.getValue().position())) {
                        return new PulsarRawdataMessageId(message.getMessageId(), position);
                    }
                    if (lastMessageId.compareTo(message.getMessageId()) == 0) {
                        return null; // last message of topic, return not found
                    }
                    if ((++i % 10000) == 0) {
                        consumer.acknowledgeCumulative(message.getMessageId());
                    }
                }
                return null; // default not found
            } finally {
                consumer.unsubscribe();
            }
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private PulsarRawdataMessageId getIdOfPositionUsingPulsarSQL(String topic, String position) {
        if (prestoDriver.get() == null) {
            try {
                prestoDriver.set(DriverManager.getDriver(prestoUrl));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        try (Connection connection = prestoDriver.get().connect(prestoUrl, prestoJdbcProperties)) {
            String queryFormat = "SELECT __message_id__ FROM pulsar.\"%s/%s\".\"%s\" WHERE position = ?";
            PreparedStatement ps = connection.prepareStatement(String.format(queryFormat, tenant, namespace, topic));
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
            return null; // not found
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
