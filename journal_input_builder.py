# -*- coding: utf-8 -*-

def _clean(value) -> str:
    return str(value or "").strip()


def build_journal_generation_input(
    service_type: str = "",
    meal_flag: str = "",
    note_text: str = "",
    memo_1: str = "",
    memo_2: str = "",
    memo_3: str = "",
    memo_4: str = "",
    memo_5: str = "",
    memo_6: str = "",
    piecework_name: str = "",
    piecework_quantity: str = "",
    piecework_steps_text: str = "",
) -> dict:
    """
    日誌入力画面の①〜⑥を、journal_rewrite.generate_journal_from_memo 用に整形する。
    画面や送信処理から独立させ、後でページ分割しても使い回せるようにする。
    """

    service_type = _clean(service_type)
    meal_flag = _clean(meal_flag)
    note_text = _clean(note_text)

    memo_1 = _clean(memo_1)
    memo_2 = _clean(memo_2)
    memo_3 = _clean(memo_3)
    memo_4 = _clean(memo_4)
    memo_5 = _clean(memo_5)
    memo_6 = _clean(memo_6)

    piecework_name = _clean(piecework_name)
    piecework_quantity = _clean(piecework_quantity)
    piecework_steps_text = _clean(piecework_steps_text)

    # サービス種別の補正
    mode_hint = service_type

    if "施設外" in service_type:
        mode_hint = "施設外"

    elif "在宅" in service_type or "在宅利用" in note_text:
        mode_hint = "在宅"

    elif "通所" in service_type or meal_flag == "あり":
        mode_hint = "通所"

    # 作業名の優先順位
    # ③入力 → 内職マスタ名 → 作業
    work_label = memo_3 or piecework_name or "作業"

    # ④に入力がなく、内職マスタ数量がある場合だけ補助的に使う
    quantity_detail = memo_4
    if not quantity_detail and piecework_name and piecework_quantity:
        quantity_detail = f"{piecework_name} {piecework_quantity}"

    parts = []

    if mode_hint:
        parts.append(f"サービス種別：{mode_hint}")

    if service_type:
        parts.append(f"画面上のサービス種別：{service_type}")

    if meal_flag:
        parts.append(f"食事提供：{meal_flag}")

    if note_text:
        parts.append(f"備考：{note_text}")

    if memo_1:
        parts.append(f"①体調：{memo_1}")

    if memo_2:
        parts.append(f"②体調に関する声掛け：{memo_2}")

    if memo_3:
        parts.append(f"③作業内容・施設外就労先：{memo_3}")

    if quantity_detail:
        parts.append(f"④数量・施設外での具体的作業内容：{quantity_detail}")

    if memo_5:
        parts.append(f"⑤支援：{memo_5}")

    if memo_6:
        parts.append(f"⑥追加メモ：{memo_6}")

    # 内職情報も残す。ただし数量は入力がある場合のみ。
    if piecework_name:
        parts.append(f"内職名：{piecework_name}")

    if piecework_quantity:
        parts.append(f"内職数量：{piecework_quantity}")

    if piecework_steps_text:
        parts.append(f"本日実施した工程：\n{piecework_steps_text}")   

    memo = "\n".join([p for p in parts if p]).strip()

    return {
        "memo": memo,
        "work_label": work_label,
        "mode_hint": mode_hint,
    }