package no.ssb.rawdata.pulsar;

import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

class PulsarRawdataMessageId {
    final MessageId messageId;
    final String position;

    PulsarRawdataMessageId(MessageId messageId, String position) {
        if (messageId == null) {
            throw new IllegalArgumentException("MessageId cannot be null");
        }
        this.messageId = messageId;
        this.position = position;
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
                ", position='" + position + '\'' +
                '}';
    }

    public String getPosition() {
        return position;
    }
}
