import struct

from utils import write_int_string


class KafkaProtocol(object):
    PRODUCE_KEY       = 0
    FETCH_KEY         = 1
    OFFSET_KEY        = 2
    METADATA_KEY      = 3
    OFFSET_COMMIT_KEY = 6
    OFFSET_FETCH_KEY  = 7

    @classmethod
    def _encode_message_header(cls, client_id, correlation_id, request_key):
        """
        Encode the common request envelope
        """
        return struct.pack('>hhih%ds' % len(client_id),
                           request_key,          # ApiKey
                           0,                    # ApiVersion
                           correlation_id,       # CorrelationId
                           len(client_id),
                           client_id)            # ClientId

    @classmethod
    def encode_metadata_request(cls, client_id, correlation_id, topics=None):
        """
        Encode a MetadataRequest

        Params
        ======
        client_id: string
        correlation_id: string
        topics: list of strings
        """
        topics = [] if topics is None else topics
        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaProtocol.METADATA_KEY)

        message += struct.pack('>i', len(topics))

        for topic in topics:
            message += struct.pack('>h%ds' % len(topic), len(topic), topic)

        return write_int_string(message)