import base64

import thrift.TSerialization
from thrift.protocol import TBinaryProtocol

def serialize(
        thrift_object,
        protocol_factory=TBinaryProtocol.TBinaryProtocolFactory()):
    """Serialize / base64 encode thrift objects.

    Args:
        thrift_object: thrift object to serialize
        protocol_factory: thrift protocol factory to use for
            serialization.
    
    Returns:
        serialized thrift object.
    """
    return base64.b64encode(thrift.TSerialization.serialize(
        thrift_object,
        protocol_factory))

def deserialize(
        base,
        buf,
        protocol_factory=TBinaryProtocol.TBinaryProtocolFactory()):
    """Deserialize / base64 decode thrift objects.

    Args:
        base: thrift object to deserialize in to.
        buf: buffer containing the serialized thrift object.
        protocol_factory: thrift protocol factory to use for
            deserialization.
    
    Returns:
        deserialized thrift object.
    """
    return thrift.TSerialization.deserialize(
            base,
            base64.b64decode(buf),
            protocol_factory)
    
