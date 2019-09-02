package no.ssb.rawdata.pulsar;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PulsarRawdataMessage implements RawdataMessage {

    long ulidMsb;
    long ulidLsb;
    String position;
    Map<String, byte[]> data;

    public PulsarRawdataMessage() {
    }

    public PulsarRawdataMessage(ULID.Value ulid, String position, Map<String, byte[]> data) {
        if (ulid == null) {
            throw new IllegalArgumentException("ulid cannot be null");
        }
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.ulidMsb = ulid.getMostSignificantBits();
        this.ulidLsb = ulid.getLeastSignificantBits();
        this.position = position;
        this.data = data;
    }

    @Override
    public ULID.Value ulid() {
        return new ULID.Value(ulidMsb, ulidLsb);
    }

    @Override
    public long sequenceNumber() {
        return 0; // non-partitioned topics already have guaranteed fifo ordering
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
        PulsarRawdataMessage that = (PulsarRawdataMessage) o;
        return ulidMsb == that.ulidMsb &&
                ulidLsb == that.ulidLsb &&
                position.equals(that.position) &&
                allArraysEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ulidMsb, ulidLsb, position, data);
    }

    private boolean allArraysEquals(PulsarRawdataMessage that) {
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            if (!that.data.containsKey(entry.getKey())) {
                return false;
            }
            if (!Arrays.equals(entry.getValue(), that.data.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessage{" +
                "ulid=" + ulid() +
                ", position='" + position + '\'' +
                ", data.keys=" + data.keySet() +
                '}';
    }

    static class Builder implements RawdataMessage.Builder {
        ULID.Value ulid;
        String position;
        Map<String, byte[]> data = new LinkedHashMap<>();

        @Override
        public synchronized PulsarRawdataMessage.Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public synchronized PulsarRawdataMessage.Builder position(String position) {
            this.position = position;
            return this;
        }

        @Override
        public synchronized PulsarRawdataMessage.Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
        }

        @Override
        public synchronized PulsarRawdataMessage build() {
            return new PulsarRawdataMessage(ulid, position, data);
        }
    }
}
