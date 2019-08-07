package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataMessageContent;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PulsarRawdataMessageContent implements RawdataMessageContent {

    final PulsarRawdataPayload payload;

    public PulsarRawdataMessageContent(String externalId, Map<String, byte[]> data) {
        if (externalId == null) {
            throw new IllegalArgumentException("externalId cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        payload = new PulsarRawdataPayload(externalId, data);
    }

    public PulsarRawdataMessageContent(PulsarRawdataPayload payload) {
        this.payload = payload;
    }

    @Override
    public String externalId() {
        return payload.externalId;
    }

    @Override
    public Set<String> keys() {
        return payload.data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return payload.data.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarRawdataMessageContent that = (PulsarRawdataMessageContent) o;
        return payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload);
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessageContent{" +
                "externalId='" + payload.externalId + '\'' +
                '}';
    }
}
