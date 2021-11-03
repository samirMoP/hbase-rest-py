import xml.etree.ElementTree as et
from hbase.utils import b64_encoder


def build_base_scanner(batch=1000, type=None, startRow=None, endRow=None, startTime=None, endTime=None, maxVersions=1):
    if type is None:
        xml = et.Element("Scanner", batch=str(batch), maxVersions=str(maxVersions))
    if type == 'row':
        xml = et.Element("Scanner",
                         batch=str(batch),
                         startRow=b64_encoder(startRow),
                         endRow=b64_encoder(endRow),
                         maxVersions=str(maxVersions))
    if type == 'time':
        xml = et.Element("Scanner",
                         batch=str(batch),
                         startTime=str(startTime),
                         endRow=str(endTime),
                         maxVersions=str(maxVersions))
    if type == 'row-time':
        xml = et.Element("Scanner",
                         batch=str(batch),
                         startRow=b64_encoder(startRow),
                         endRow=b64_encoder(endRow),
                         startTime=str(startTime),
                         endRo=str(endTime),
                         maxVersions=str(maxVersions)
                         )

    return et.tostring(xml).decode('utf-8')


def build_prefix_filter(row_perfix, batch=1000, type=None, startRow=None,
                        endRow=None, startTime=None, endTime=None, maxVersions=1):
    if type is None:
        xml = et.Element("Scanner", batch=str(batch), maxVersions=str(maxVersions))
        filter = et.SubElement(xml, "filter")
        filter.text = '{"type":"PrefixFilter", "value":"%s"}'%(b64_encoder(row_perfix))
    return et.tostring(xml).decode('utf-8')


