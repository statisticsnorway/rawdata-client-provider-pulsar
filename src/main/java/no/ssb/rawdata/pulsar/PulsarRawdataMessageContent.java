package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PulsarRawdataMessageContent implements RawdataMessage {

    String position;
    Map<String, byte[]> data;

    public PulsarRawdataMessageContent() {
    }

    public PulsarRawdataMessageContent(String position, Map<String, byte[]> data) {
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.position = position;
        this.data = data;
    }

    @Override
    public String position() {
        return position;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public void setData(Map<String, byte[]> data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarRawdataMessageContent that = (PulsarRawdataMessageContent) o;
        return Objects.equals(position, that.position) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, data);
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessageContent{" +
                "position='" + position + '\'' +
                '}';
    }
}
