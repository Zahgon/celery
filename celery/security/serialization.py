"""Secure serializer."""
from kombu.serialization import dumps, loads, registry
from kombu.utils.encoding import bytes_to_str, ensure_bytes, str_to_bytes

from celery.app.defaults import DEFAULT_SECURITY_DIGEST
from celery.utils.serialization import b64decode, b64encode

from .certificate import Certificate, FSCertStore
from .key import PrivateKey
from .utils import get_digest_algorithm, reraise_errors

__all__ = ('SecureSerializer', 'register_auth')

# Note: we guarantee that this value won't appear in the serialized data,
# so we can use it as a separator.
# If you change this value, make sure it's not present in the serialized data.
DEFAULT_SEPARATOR = str_to_bytes("\x00\x01")


class SecureSerializer:
    """Signed serializer."""

    def __init__(self, key=None, cert=None, cert_store=None,
                 digest=DEFAULT_SECURITY_DIGEST, serializer='json'):
        self._key = key
        self._cert = cert
        self._cert_store = cert_store
        self._digest = get_digest_algorithm(digest)
        self._serializer = serializer

    def serialize(self, data):
        """Serialize data structure into string."""
        pass

    def deserialize(self, data):
        """Deserialize data structure from string."""
        pass

    def _pack(self, body, content_type, content_encoding, signer, signature,
              sep=DEFAULT_SEPARATOR):
        pass

    def _unpack(self, payload, sep=DEFAULT_SEPARATOR):
        pass


def register_auth(key=None, key_password=None, cert=None, store=None,
                  digest=DEFAULT_SECURITY_DIGEST,
                  serializer='json'):
    """Register security serializer."""
    s = SecureSerializer(key and PrivateKey(key, password=key_password),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         digest, serializer=serializer)
    registry.register('auth', s.serialize, s.deserialize,
                      content_type='application/data',
                      content_encoding='utf-8')
