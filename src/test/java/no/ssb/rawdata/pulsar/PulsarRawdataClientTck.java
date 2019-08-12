package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class PulsarRawdataClientTck {

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() throws PulsarAdminException, PulsarClientException {
        Map<String, String> configuration = Map.of(
                "pulsar.service.url", "pulsar://localhost:6650",
                "pulsar.tenant", "test",
                "pulsar.namespace", "rawdata",
                "pulsar.producer", "tck-testng"
        );
        client = ProviderConfigurator.configure(configuration, "pulsar", RawdataClientInitializer.class);

        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(new AuthenticationDisabled())
                .build()) {

            String tenant = configuration.get("pulsar.tenant");
            String namespace = configuration.get("pulsar.namespace");

            Tenants tenants = admin.tenants();
            Namespaces namespaces = admin.namespaces();
            Topics topics = admin.topics();
            List<String> existingTenants = tenants.getTenants();
            if (!existingTenants.contains(tenant)) {
                // create missing tenant
                tenants.createTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            }
            List<String> existingNamespaces = namespaces.getNamespaces(tenant);
            if (!existingNamespaces.contains(tenant + "/" + namespace)) {
                // create missing namespace
                namespaces.createNamespace(tenant + "/" + namespace);
                namespaces.setRetention(tenant + "/" + namespace, new RetentionPolicies(-1, -1));
            }

            // delete all topics in namespace
            List<String> existingTopics = namespaces.getTopics(tenant + "/" + namespace);
            for (String topic : existingTopics) {
                for (String subscription : topics.getSubscriptions(topic)) {
                    topics.deleteSubscription(topic, subscription);
                }
                topics.delete(topic);
                admin.schemas().deleteSchema(topic);
            }
        }
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeReadByProducer() {
        RawdataProducer producer = client.producer("the-topic");

        assertEquals(producer.lastPosition(), null);
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        RawdataProducer producer = client.producer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        producer.publish("a", "b");

        assertEquals(producer.lastPosition(), "b");

        producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish("c");

        assertEquals(producer.lastPosition(), "c");
    }

    @Test(expectedExceptions = RawdataContentNotBufferedException.class)
    public void thatPublishNonBufferedMessagesThrowsException() {
        RawdataProducer producer = client.producer("the-topic");
        producer.publish("unbuffered-1");
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.publish(expected1.position());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message, expected1);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.publish(expected1.position());

        RawdataMessage message = future.join();
        assertEquals(message, expected1);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1, expected1);
        assertEquals(message2, expected2);
        assertEquals(message3, expected3);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0), expected1);
        assertEquals(messages.get(1), expected2);
        assertEquals(messages.get(2), expected3);
    }

    private CompletableFuture<List<RawdataMessage>> receiveAsyncAddMessageAndRepeatRecursive(RawdataConsumer consumer, String endPosition, List<RawdataMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.position())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer1 = client.consumer("the-topic");
        RawdataConsumer consumer2 = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<RawdataMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0), expected1);
        assertEquals(messages1.get(1), expected2);
        assertEquals(messages1.get(2), expected3);

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0), expected1);
        assertEquals(messages2.get(1), expected2);
        assertEquals(messages2.get(2), expected3);
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d")) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }
}
