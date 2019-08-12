/**
 * Copied from Apache Pulsar source and modified.
 */
package no.ssb.rawdata.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.testng.Assert.assertEquals;

/**
 * Test Pulsar Schema.
 */
public class SchemaTest {

    Logger log = LoggerFactory.getLogger(SchemaTest.class);

    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeMethod
    public void setup() throws Exception {
        this.client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(new AuthenticationDisabled())
                .build();
    }

    @Test
    public void testCreateSchemaAfterDeletion() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + new Random().nextInt(1000000);
        final String topic = "test-create-schema-after-deletion";
        final String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Set.of("standalone")
        );

        // Create a topic with `Person`
        try (Producer<Schemas.Person> producer = client.newProducer(Schema.JSON(Schemas.Person.class))
                .topic(fqtn)
                .create()
        ) {
            Schemas.Person person = new Schemas.Person();
            person.setName("Tom Hanks");
            person.setAge(60);

            producer.send(person);

            log.info("Successfully published person : {}", person);
        }

        try (Consumer<Schemas.Person> consumer = client.newConsumer(Schema.JSON(Schemas.Person.class))
                .topic(fqtn)
                .subscriptionName("s1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            Message<Schemas.Person> message = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            assertEquals(message.getValue().getName(), "Tom Hanks");
        }

        log.info("Deleting schema of topic {}", fqtn);
        // delete the schema
        admin.schemas().deleteSchema(fqtn);
        log.info("Successfully deleted schema of topic {}", fqtn);

        // after deleting the topic, try to create a topic with a different schema
        try (Producer<Schemas.Student> producer = client.newProducer(Schema.AVRO(Schemas.Student.class))
                .topic(fqtn)
                .create()
        ) {
            Schemas.Student student = new Schemas.Student();
            student.setName("Tom Jerry");
            student.setAge(30);
            student.setGpa(6);
            student.setGpa(10);

            producer.send(student);

            log.info("Successfully published student : {}", student);
        }

        try (Consumer<Schemas.Student> consumer = client.newConsumer(Schema.AVRO(Schemas.Student.class))
                .topic(fqtn)
                .subscriptionName("s1")
                .subscribe()) {
            Message<Schemas.Student> message = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            assertEquals(message.getValue().getName(), "Tom Jerry");
        }
    }
}