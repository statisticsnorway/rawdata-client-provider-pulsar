package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataMessageId;
import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

class PulsarRawdataMessageId implements RawdataMessageId {
    final MessageId messageId;
    final String externalId;

    PulsarRawdataMessageId(MessageId messageId, String externalId) {
        if (messageId == null) {
            throw new IllegalArgumentException("MessageId cannot be null");
        }
        this.messageId = messageId;
        this.externalId = externalId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarRawdataMessageId that = (PulsarRawdataMessageId) o;
        return messageId.equals(that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessageId{" +
                "messageId=" + messageId +
                ", externalId='" + externalId + '\'' +
                '}';
    }

    public String getExternalId() {
        return externalId;
    }
}
