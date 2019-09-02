import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.pulsar.PulsarRawdataClientInitializer;

module no.ssb.rawdata.pulsar {
    requires no.ssb.rawdata.api;
    requires pulsar.client.api;
    requires pulsar.client.admin;
    requires java.sql;
    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;

    provides RawdataClientInitializer with PulsarRawdataClientInitializer;
}
