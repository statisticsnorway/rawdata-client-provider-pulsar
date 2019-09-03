package no.ssb.rawdata.pulsar;

import no.ssb.rawdata.api.RawdataCursor;
import org.apache.pulsar.client.api.MessageId;

class PulsarCursor implements RawdataCursor {

    final MessageId messageId;
    final boolean inclusive;

    PulsarCursor(MessageId messageId, boolean inclusive) {
        this.messageId = messageId;
        this.inclusive = inclusive;
    }
}
