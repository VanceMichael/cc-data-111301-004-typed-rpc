from pathlib import Path
from google.protobuf.descriptor_pb2 import FileDescriptorSet


def inspect_file(path):
    bundle = FileDescriptorSet()
    bundle.ParseFromString(Path(path).read_bytes())
    if not bundle.file:
        raise ValueError('描述符集合为空')
    return {'files':[{'name':file.name,'package':file.package,'dependencies':list(file.dependency),
                      'messages':[message.name for message in file.message_type]} for file in bundle.file]}
