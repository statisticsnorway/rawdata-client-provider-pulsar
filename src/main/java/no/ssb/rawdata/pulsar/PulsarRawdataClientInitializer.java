package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.util.Map;
import java.util.Set;

@ProviderName("pulsar")
public class PulsarRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "pulsar";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "pulsar.admin.url",
                "pulsar.broker.url",
                "pulsar.tenant",
                "pulsar.namespace",
                "pulsar.producer"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        try {
            PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(configuration.get("pulsar.broker.url"))
                    .build();

            PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                    .serviceHttpUrl(configuration.get("pulsar.admin.url"))
                    .authentication(new AuthenticationDisabled())
                    .build();

            String tenant = configuration.get("pulsar.tenant");
            String namespace = configuration.get("pulsar.namespace");
            String producerName = configuration.get("pulsar.producer");

            return new PulsarRawdataClient(pulsarAdmin, pulsarClient, tenant, namespace, producerName);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
