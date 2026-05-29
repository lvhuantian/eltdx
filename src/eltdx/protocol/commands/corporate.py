"""Corporate action and finance command builders and parsers."""

from __future__ import annotations

import struct

from eltdx.exceptions import ProtocolError
from eltdx.models import CapitalChangeBlock, CapitalChangeRecord, FinanceBatch, FinanceRecord
from eltdx.protocol.constants import TYPE_CAPITAL_CHANGES, TYPE_FINANCE_BATCH
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import date_from_yyyymmdd, get_volume, little_f32, little_u16, little_u32, split_code

CAPITAL_CHANGE_RECORD_SIZE = 29
FINANCE_RECORD_SIZE = 143
FINANCE_INFO_SIZE = 136

CAPITAL_CHANGE_CATEGORY_NAMES = {
    1: "除权除息",
    2: "送配股上市",
    3: "非流通股上市",
    4: "国家股配售",
    5: "股本变化",
    6: "增发新股",
    7: "股份回购",
    8: "增发新股上市",
    9: "转配股上市",
    10: "可转债上市",
    11: "扩缩股",
    12: "非流通股缩股",
    13: "送认购权证",
    14: "送认沽权证",
    15: "重整调整",
}
FLOAT_VALUE_CATEGORIES = {1, 11, 12, 13, 14, 15}


def build_capital_changes_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    data = b"\x01\x00" + bytes([market_id]) + number.encode("ascii")
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_CAPITAL_CHANGES, data=data)


def parse_capital_changes_payload(response: ResponseFrame, request_payload: dict | None = None) -> CapitalChangeBlock:
    payload = response.data
    if len(payload) < 11:
        raise ProtocolError("invalid capital changes payload")

    block_count = little_u16(payload[:2])
    market_id = payload[2]
    exchange = {0: "sz", 1: "sh", 2: "bj"}.get(market_id, "unknown")
    code = payload[3:9].decode("ascii")
    record_count = little_u16(payload[9:11])
    expected_length = 11 + record_count * CAPITAL_CHANGE_RECORD_SIZE
    if len(payload) < expected_length:
        raise ProtocolError("truncated capital changes payload")

    records: list[CapitalChangeRecord] = []
    offset = 11
    for _ in range(record_count):
        record = payload[offset : offset + CAPITAL_CHANGE_RECORD_SIZE]
        offset += CAPITAL_CHANGE_RECORD_SIZE
        records.append(_parse_capital_change_record(record))
    if offset != len(payload):
        raise ProtocolError(f"unexpected trailing capital changes payload bytes: {len(payload) - offset}")

    return CapitalChangeBlock(
        exchange=exchange,
        market_id=market_id,
        code=code,
        block_count=block_count,
        records=tuple(records),
        raw_payload=payload,
    )


def build_finance_batch_frame(payload: dict, msg_id: int) -> RequestFrame:
    codes = payload.get("codes", [])
    if isinstance(codes, str):
        codes = [codes]
    code_list = list(codes)
    if len(code_list) > 0xFFFF:
        raise ValueError("too many codes")
    data = bytearray(len(code_list).to_bytes(2, "little"))
    for code in code_list:
        market_id, _, number = split_code(code)
        data.append(market_id)
        data.extend(number.encode("ascii"))
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_FINANCE_BATCH, data=bytes(data))


def parse_finance_batch_payload(response: ResponseFrame, request_payload: dict | None = None) -> FinanceBatch:
    payload = response.data
    if len(payload) < 2:
        raise ProtocolError("invalid finance batch payload")
    record_count = little_u16(payload[:2])
    expected_length = 2 + record_count * FINANCE_RECORD_SIZE
    if len(payload) != expected_length:
        raise ProtocolError(f"invalid finance batch length: expected {expected_length}, got {len(payload)}")

    records: list[FinanceRecord] = []
    offset = 2
    for _ in range(record_count):
        record = payload[offset : offset + FINANCE_RECORD_SIZE]
        offset += FINANCE_RECORD_SIZE
        records.append(_parse_finance_record(record))
    return FinanceBatch(records=tuple(records), raw_payload=payload)


def _parse_capital_change_record(record: bytes) -> CapitalChangeRecord:
    if len(record) != CAPITAL_CHANGE_RECORD_SIZE:
        raise ProtocolError("invalid capital change record length")
    market_id = record[0]
    exchange = {0: "sz", 1: "sh", 2: "bj"}.get(market_id, "unknown")
    code = record[1:7].decode("ascii")
    date_raw = little_u32(record[8:12])
    category_raw = record[12]
    c1_raw = record[13:17]
    c2_raw = record[17:21]
    c3_raw = record[21:25]
    c4_raw = record[25:29]
    c1_float = little_f32(c1_raw)
    c2_float = little_f32(c2_raw)
    c3_float = little_f32(c3_raw)
    c4_float = little_f32(c4_raw)
    c1_value, c2_value, c3_value, c4_value = _decode_capital_change_values(
        category_raw,
        c1_raw,
        c2_raw,
        c3_raw,
        c4_raw,
        c1_float,
        c2_float,
        c3_float,
        c4_float,
    )
    return CapitalChangeRecord(
        exchange=exchange,
        market_id=market_id,
        code=code,
        reserved_7=record[7],
        date_raw=date_raw,
        date=date_from_yyyymmdd(date_raw),
        category_raw=category_raw,
        category_name=CAPITAL_CHANGE_CATEGORY_NAMES.get(category_raw),
        c1_raw=c1_raw,
        c2_raw=c2_raw,
        c3_raw=c3_raw,
        c4_raw=c4_raw,
        c1_float=c1_float,
        c2_float=c2_float,
        c3_float=c3_float,
        c4_float=c4_float,
        c1_value=c1_value,
        c2_value=c2_value,
        c3_value=c3_value,
        c4_value=c4_value,
        record_hex=record.hex(),
    )


def _decode_capital_change_values(
    category_raw: int,
    c1_raw: bytes,
    c2_raw: bytes,
    c3_raw: bytes,
    c4_raw: bytes,
    c1_float: float,
    c2_float: float,
    c3_float: float,
    c4_float: float,
) -> tuple[float, float, float, float]:
    if category_raw in FLOAT_VALUE_CATEGORIES:
        return c1_float, c2_float, c3_float, c4_float
    return (
        get_volume(little_u32(c1_raw)) * 10000.0,
        get_volume(little_u32(c2_raw)) * 10000.0,
        get_volume(little_u32(c3_raw)) * 10000.0,
        get_volume(little_u32(c4_raw)) * 10000.0,
    )


def _parse_finance_record(record: bytes) -> FinanceRecord:
    if len(record) != FINANCE_RECORD_SIZE:
        raise ProtocolError("invalid finance record length")
    market_id = record[0]
    exchange = {0: "sz", 1: "sh", 2: "bj"}.get(market_id, "unknown")
    code = record[1:7].decode("ascii")
    info = record[7:143]
    if len(info) != FINANCE_INFO_SIZE:
        raise ProtocolError("invalid finance info length")

    (
        liu_tong_gu_ben,
        province_raw,
        industry_raw,
        updated_date_raw,
        ipo_date_raw,
        zong_gu_ben,
        guo_jia_gu,
        fa_qi_ren_fa_ren_gu,
        fa_ren_gu,
        b_gu,
        h_gu,
        eps,
        zong_zi_chan,
        liu_dong_zi_chan,
        gu_ding_zi_chan,
        wu_xing_zi_chan,
        gu_dong_ren_shu,
        liu_dong_fu_zhai,
        chang_qi_fu_zhai,
        zi_ben_gong_ji_jin,
        jing_zi_chan,
        zhu_ying_shou_ru,
        zhu_ying_li_run,
        ying_shou_zhang_kuan,
        ying_ye_li_run,
        tou_zi_shou_yu,
        jing_ying_xian_jin_liu,
        zong_xian_jin_liu,
        cun_huo,
        li_run_zong_he,
        shui_hou_li_run,
        jing_li_run,
        wei_fen_li_run,
        mei_gu_jing_zi_chan,
        bao_liu_2,
    ) = struct.unpack("<fHHII30f", info)

    return FinanceRecord(
        exchange=exchange,
        market_id=market_id,
        code=code,
        finance_info_raw=info,
        liu_tong_gu_ben_raw_float=liu_tong_gu_ben,
        province_raw=province_raw,
        industry_raw=industry_raw,
        updated_date_raw=updated_date_raw,
        updated_date=date_from_yyyymmdd(updated_date_raw),
        ipo_date_raw=ipo_date_raw,
        ipo_date=date_from_yyyymmdd(ipo_date_raw),
        zong_gu_ben_raw_float=zong_gu_ben,
        guo_jia_gu_raw_float=guo_jia_gu,
        fa_qi_ren_fa_ren_gu_raw_float=fa_qi_ren_fa_ren_gu,
        fa_ren_gu_raw_float=fa_ren_gu,
        b_gu_raw_float=b_gu,
        h_gu_raw_float=h_gu,
        eps_raw=eps,
        zong_zi_chan_raw_float=zong_zi_chan,
        liu_dong_zi_chan_raw_float=liu_dong_zi_chan,
        gu_ding_zi_chan_raw_float=gu_ding_zi_chan,
        wu_xing_zi_chan_raw_float=wu_xing_zi_chan,
        gu_dong_ren_shu_raw_float=gu_dong_ren_shu,
        liu_dong_fu_zhai_raw_float=liu_dong_fu_zhai,
        chang_qi_fu_zhai_raw_float=chang_qi_fu_zhai,
        zi_ben_gong_ji_jin_raw_float=zi_ben_gong_ji_jin,
        jing_zi_chan_raw_float=jing_zi_chan,
        zhu_ying_shou_ru_raw_float=zhu_ying_shou_ru,
        zhu_ying_li_run_raw_float=zhu_ying_li_run,
        ying_shou_zhang_kuan_raw_float=ying_shou_zhang_kuan,
        ying_ye_li_run_raw_float=ying_ye_li_run,
        tou_zi_shou_yu_raw_float=tou_zi_shou_yu,
        jing_ying_xian_jin_liu_raw_float=jing_ying_xian_jin_liu,
        zong_xian_jin_liu_raw_float=zong_xian_jin_liu,
        cun_huo_raw_float=cun_huo,
        li_run_zong_he_raw_float=li_run_zong_he,
        shui_hou_li_run_raw_float=shui_hou_li_run,
        jing_li_run_raw_float=jing_li_run,
        wei_fen_li_run_raw_float=wei_fen_li_run,
        mei_gu_jing_zi_chan_raw_float=mei_gu_jing_zi_chan,
        bao_liu_2_raw_float=bao_liu_2,
        record_hex=record.hex(),
    )
