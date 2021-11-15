import xml.etree.ElementTree as et
from hbase.utils import b64_encoder


# {"type":"SkipFilter","filters":[{"op":"NOT_EQUAL","type":"QualifierFilter","comparator":{"value":"dGVzdFF1YWxpZmllck9uZS0y","type":"BinaryComparator"}}]}
# {"op":"MUST_PASS_ALL","type":"FilterList","filters":[{"op":"EQUAL","type":"RowFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"QualifierFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"one","type":"SubstringComparator"}}]}
# {"op":"MUST_PASS_ONE","type":"FilterList","filters":[{"op":"EQUAL","type":"RowFilter","comparator":{"value":".+Two.+","type":"RegexStringComparator"}},{"op":"EQUAL","type":"QualifierFilter","comparator":{"value":".+-2","type":"RegexStringComparator"}},{"op":"EQUAL","type":"ValueFilter","comparator":{"value":"one","type":"SubstringComparator"}}]}


def build_base_xml(
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    if type is None:
        xml = et.Element("Scanner", batch=str(batch), maxVersions=str(maxVersions))
    if type == "row":
        xml = et.Element(
            "Scanner",
            batch=str(batch),
            startRow=b64_encoder(startRow),
            endRow=b64_encoder(endRow),
            maxVersions=str(maxVersions),
        )
    if type == "time":
        xml = et.Element(
            "Scanner",
            batch=str(batch),
            startTime=str(startTime),
            endRow=str(endTime),
            maxVersions=str(maxVersions),
        )
    if type == "row-time":
        xml = et.Element(
            "Scanner",
            batch=str(batch),
            startRow=b64_encoder(startRow),
            endRow=b64_encoder(endRow),
            startTime=str(startTime),
            endRo=str(endTime),
            maxVersions=str(maxVersions),
        )
    if isinstance(column, list) and len(column) > 0:
        for c in column:
            column = et.SubElement(xml, "column")
            column.text = b64_encoder(c)
    return xml


def build_base_scanner(
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )
    return et.tostring(xml).decode("utf-8")


def build_prefix_filter(
    row_perfix,
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )

    filter = et.SubElement(xml, "filter")
    filter.text = '{"type":"PrefixFilter", "value":"%s"}' % (b64_encoder(row_perfix))
    return et.tostring(xml).decode("utf-8")


def build_row_filter(
    row_value,
    operation,
    comparator="binary",
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    # {"op": "LESS", "type": "RowFilter", "comparator": {"value": "dGVzdFJvd09uZS0y", "type": "BinaryComparator"}}
    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )

    filter = et.SubElement(xml, "filter")
    if comparator == "binary":
        filter.text = (
            '{"op":"%s","type":"RowFilter", "comparator": {"value": "%s", "type": "BinaryComparator"} }'
            % (operation, b64_encoder(row_value))
        )
    else:
        filter.text = (
            '{"op":"%s","type":"RowFilter", "comparator": {"value": "%s", "type": "RegexStringComparator"} }'
            % (operation, row_value)
        )

    return et.tostring(xml).decode("utf-8")


def build_value_filter(
    value,
    operation,
    comparator="binary",
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    # {"op":"EQUAL","type":"ValueFilter","comparator":{"value":"dGVzdFZhbHVlT25l","type":"BinaryComparator"}}

    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )
    filter = et.SubElement(xml, "filter")
    if comparator == "binary":
        filter.text = (
            '{"op":"%s","type":"ValueFilter", "comparator": {"value": "%s", "type": "BinaryComparator"} }'
            % (operation, b64_encoder(value))
        )
    else:
        filter.text = (
            '{"op":"%s","type":"ValueFilter", "comparator": {"value": "%s", "type": "SubstringComparator"}}'  # SubstringComparator
            % (operation, value)
        )

    return et.tostring(xml).decode("utf-8")


def build_single_column_value_filter(
    family,
    qualifier,
    value,
    operation,
    comparator="binary",
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    """
    {
      "type": "SingleColumnValueFilter",
      "op": "EQUAL",
      "family": "ZmFtaWx5",
      "qualifier": "Y29sMQ\u003d\u003d",
      "latestVersion": true,
      "comparator": {
        "type": "BinaryComparator",
        "value": "MQ\u003d\u003d"
      }
    }
        :param operation:
        :param comparator:
        :param batch:
        :param type:
        :param startRow:
        :param endRow:
        :param startTime:
        :param endTime:
        :param maxVersions:
        :return:
    """
    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )
    filter = et.SubElement(xml, "filter")
    if comparator == "binary":
        filter.text = '{"op":"%s","type":"SingleColumnValueFilter","family":"%s", "qualifier":"%s", "comparator": {"value": "%s", "type": "BinaryComparator"}}' % (
            operation,
            b64_encoder(family),
            b64_encoder(qualifier),
            b64_encoder(value),
        )

    else:
        filter.text = (
            '{"op":"%s","type":"SingleColumnValueFilter","family":"%s", "qualifier":"%s", "comparator": {"value": "%s", "type": "SubstringComparator"}}'
            % (operation, b64_encoder(family), b64_encoder(qualifier), value)
        )

    return et.tostring(xml).decode("utf-8")


def build_column_prefix_filter(
    column_prefix,
    batch=1000,
    type=None,
    startRow=None,
    endRow=None,
    startTime=None,
    endTime=None,
    maxVersions=1,
    column=None,
):
    xml = build_base_xml(
        batch, type, startRow, endRow, startTime, endTime, maxVersions, column
    )
    filter = et.SubElement(xml, "filter")
    filter.text = '{"type":"ColumnPrefixFilter", "value": "%s"}' % (
        b64_encoder(column_prefix)
    )
    return et.tostring(xml).decode("utf-8")
