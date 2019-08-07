package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Objects;

public class PulsarRawdataMessage implements RawdataMessage {
    private final PulsarRawdataMessageId id;
    private final PulsarRawdataMessageContent content;

    public PulsarRawdataMessage(PulsarRawdataMessageId id, PulsarRawdataMessageContent content) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        if (content == null) {
            throw new IllegalArgumentException("content cannot be null");
        }
        this.id = id;
        this.content = content;
    }

    @Override
    public PulsarRawdataMessageId id() {
        return id;
    }

    @Override
    public PulsarRawdataMessageContent content() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarRawdataMessage that = (PulsarRawdataMessage) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessage{" +
                "id=" + id +
                ", content=" + content +
                '}';
    }
}
