package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

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
                "pulsar.service.url",
                "pulsar.tenant",
                "pulsar.namespace",
                "pulsar.producer",
                "pulsar.consumer"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String serviceUrl = configuration.get("pulsar.service.url");
        PulsarClient pulsarClient;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        String tenant = configuration.get("pulsar.tenant");
        String namespace = configuration.get("pulsar.namespace");
        String producerName = configuration.get("pulsar.producer");
        String consumerName = configuration.get("pulsar.consumer");

        return new PulsarRawdataClient(pulsarClient, tenant, namespace, producerName, consumerName);
    }
}
