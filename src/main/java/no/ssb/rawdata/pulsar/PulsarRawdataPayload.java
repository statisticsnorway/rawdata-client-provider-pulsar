package no.ssb.rawdata.pulsar;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class PulsarRawdataPayload {

    String externalId;
    Map<String, byte[]> data;

    public PulsarRawdataPayload() {
    }

    public PulsarRawdataPayload(String externalId, Map<String, byte[]> data) {
        this.externalId = externalId;
        this.data = data;
    }

    @Override
    public String toString() {
        return "PulsarRawdataPayload{" +
                "externalId='" + externalId + '\'' +
                ", data=" + data +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarRawdataPayload that = (PulsarRawdataPayload) o;
        return externalId.equals(that.externalId) &&
                data.keySet().equals(((PulsarRawdataPayload) o).data.keySet()) &&
                allArraysEquals(that);
    }

    private boolean allArraysEquals(PulsarRawdataPayload that) {
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            if (!Arrays.equals(entry.getValue(), that.data.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(externalId);
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public void setData(Map<String, byte[]> data) {
        this.data = data;
    }
}
