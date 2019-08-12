package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class SchemaDeleteTest {

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() throws PulsarAdminException, PulsarClientException {
        Map<String, String> configuration = Map.of(
                "pulsar.service.url", "pulsar://localhost:6650",
                "pulsar.tenant", "test",
                "pulsar.namespace", "rawdata",
                "pulsar.producer", "tck-testng",
                "pulsar.consumer", "tck-testng"
        );
        client = ProviderConfigurator.configure(configuration, "pulsar", RawdataClientInitializer.class);

        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(new AuthenticationDisabled())
                .build();

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

        Schemas schemas = admin.schemas();

        // delete all topics in namespace
        List<String> existingTopics = namespaces.getTopics(tenant + "/" + namespace);
        for (String topic : existingTopics) {
            //SchemaInfo schemaInfo = schemas.getSchemaInfo(topic);
            //System.out.printf("SchemaInfo: %s", schemaInfo);
            topics.delete(topic);
            schemas.deleteSchema(topic);
        }

        namespaces.deleteNamespace(tenant + "/" + namespace);

        tenants.deleteTenant(tenant);
        tenants.createTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));

        namespaces.createNamespace(tenant + "/" + namespace);
        namespaces.setRetention(tenant + "/" + namespace, new RetentionPolicies(-1, -1));
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        RawdataMessageContent expected1 = producer.buffer(producer.builder().externalId("a").put("payload", new byte[5]));
        producer.publish(expected1.externalId());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.content(), expected1);
    }

    @Test
    public void XthatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        RawdataMessageContent expected1 = producer.buffer(producer.builder().externalId("a").put("payload", new byte[5]));
        producer.publish(expected1.externalId());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.content(), expected1);
    }
}
