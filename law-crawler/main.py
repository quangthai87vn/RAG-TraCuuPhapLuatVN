import os
import json
import uuid
from bs4 import BeautifulSoup

from db import db
from models.models import (
    PDChuDe, PDDeMuc, PDChuong, PDDieu, PDTable, PDFile, PDMucLienQuan
)

from helper import convert_roman_to_num, extract_input


# ====== Config ======
CHUDE_PATH = "./phap-dien/chude.json"
DEMUC_PATH = "./phap-dien/demuc.json"
TREENODE_PATH = "./phap-dien/treeNode.json"
DEMUC_DIR = "./phap-dien/demuc"

# Muốn chạy full thì để None
CHECKPOINT =  "d8e4a3a0-254c-4593-967c-214ae12bcb0f.html"


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def text_of(tag):
    if tag is None:
        return ""
    return tag.get_text(" ", strip=True)


def next_sibling_skip_ws(node):
    sib = node.next_sibling
    while sib is not None and str(sib).strip() == "":
        sib = sib.next_sibling
    return sib


def main():
    # ===== 0) Reset DB =====
    db.connect(reuse_if_open=True)
    db.drop_tables([PDMucLienQuan, PDTable, PDFile, PDDieu, PDChuong, PDDeMuc, PDChuDe], safe=True)
    db.create_tables([PDChuDe, PDDeMuc, PDChuong, PDDieu, PDFile, PDTable, PDMucLienQuan], safe=True)

    # ===== 1) Load JSON =====
    chudes = read_json(CHUDE_PATH)
    demucs = read_json(DEMUC_PATH)
    tree_nodes = read_json(TREENODE_PATH)

    # Map nhanh
    demuc_to_chude = {d["Value"]: d.get("ChuDe") for d in demucs if "Value" in d}

    # ===== 2) Insert pdchude =====
    print("Insert pdchude...")
    for c in chudes:
        cid = c.get("Value")
        if not cid:
            continue
        PDChuDe.insert(
            id=cid,
            ten=c.get("Text", ""),
            stt=safe_int(c.get("STT"), 0),
        ).on_conflict_ignore().execute()
    print("✅ pdchude done")

    # ===== 3) Insert pddemuc =====
    print("Insert pddemuc...")
    for d in demucs:
        did = d.get("Value")
        if not did:
            continue
        chude_uuid = d.get("ChuDe")

        # Nếu demuc trỏ tới chude không tồn tại -> log luôn
        if chude_uuid and PDChuDe.get_or_none(PDChuDe.id == chude_uuid) is None:
            print(f"⚠️ demuc {did} trỏ tới chude {chude_uuid} nhưng pdchude chưa có (skip)")
            continue

        PDDeMuc.insert(
            id=did,
            ten=d.get("Text", ""),
            stt=safe_int(d.get("STT"), 0),
            chude_id=chude_uuid,
        ).on_conflict_ignore().execute()
    print("✅ pddemuc done")

    # ===== 4) Crawl HTML từng đề mục =====
    files = sorted([f for f in os.listdir(DEMUC_DIR) if f.endswith(".html")])

    isSkipping = CHECKPOINT is not None
    lienquan_pairs = []

    for file_name in files:
        if isSkipping:
            if file_name == CHECKPOINT:
                isSkipping = False
            else:
                continue

        demuc_id = file_name.replace(".html", "")
        demuc_obj = PDDeMuc.get_or_none(PDDeMuc.id == demuc_id)
        if demuc_obj is None:
            print(f"⚠️ Skip {file_name}: demuc_id={demuc_id} chưa có trong DB")
            continue

        chude_id = demuc_to_chude.get(demuc_id)
        if chude_id is None:
            print(f"⚠️ Skip {file_name}: không map được chude cho demuc {demuc_id}")
            continue

        # Lấy các node thuộc đề mục này
        demuc_nodes = [n for n in tree_nodes if n.get("DeMucID") == demuc_id]
        if not demuc_nodes:
            print(f"⚠️ Không có tree node cho {file_name}")
            continue

        # Đọc HTML
        html_path = os.path.join(DEMUC_DIR, file_name)
        with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        # --- chương nodes
        chuong_nodes = [n for n in demuc_nodes if str(n.get("TEN", "")).startswith("Chương ")]
        chuong_mapcs = []

        for cn in chuong_nodes:
            mapc = cn.get("MAPC")
            if not mapc:
                continue
            PDChuong.insert(
                mapc=mapc,
                ten=cn.get("TEN", ""),
                demuc_id=demuc_id,
                chimuc=str(cn.get("ChiMuc", "")),
                stt=convert_roman_to_num(cn.get("ChiMuc")),
            ).on_conflict_ignore().execute()
            chuong_mapcs.append(mapc)

        # Nếu không có chương -> tạo chương giả
        if not chuong_mapcs:
            fake_mapc = str(uuid.uuid4())
            PDChuong.insert(
                mapc=fake_mapc,
                ten="",
                demuc_id=demuc_id,
                chimuc="0",
                stt=0,
            ).on_conflict_ignore().execute()
            chuong_mapcs.append(fake_mapc)

        # --- dieu nodes (các node còn lại)
        dieu_nodes = [n for n in demuc_nodes if n not in chuong_nodes and n.get("MAPC")]
        print(f"📄 {file_name}: {len(chuong_mapcs)} chương | {len(dieu_nodes)} điều")

        stt_dieu = 0
        for dn in dieu_nodes:
            mapc = dn.get("MAPC")
            if not mapc:
                continue

            # chọn chương theo prefix MAPC (fallback: chương đầu tiên)
            chuong_id = None
            for cm in chuong_mapcs:
                if str(mapc).startswith(str(cm)):
                    chuong_id = cm
                    break
            if chuong_id is None:
                chuong_id = chuong_mapcs[0]

            # tìm anchor
            a = soup.select_one(f'a[name="{mapc}"]')
            if a is None:
                # không có anchor thì bỏ qua
                # (đây là lý do hay làm pddieu rỗng)
                # print(f"⚠️ Không thấy anchor {mapc} trong {file_name}")
                continue

            # tên điều: thường là text ngay sau <a ...></a>
            ten = ""
            sib = next_sibling_skip_ws(a)
            if sib is not None:
                ten = str(sib).strip()
            if not ten:
                ten = dn.get("TEN", "")

            # tìm p chứa anchor
            p_anchor = a.find_parent("p")

            # ghi chú VBQPPL
            p_ghichu = None
            if p_anchor:
                p_ghichu = p_anchor.find_next_sibling("p", {"class": "pGhiChu"})
            vbqppl = text_of(p_ghichu)
            vbqppl_link = None
            if p_ghichu:
                link_tag = p_ghichu.select_one("a[href]")
                if link_tag:
                    vbqppl_link = link_tag.get("href")

            # nội dung
            p_noidung = None
            if p_anchor:
                p_noidung = p_anchor.find_next_sibling("p", {"class": "pNoiDung"})
            if p_noidung is None:
                # fallback
                p_noidung = a.find_parent().find_next("p", {"class": "pNoiDung"})

            noidung = ""
            tables = []
            if p_noidung:
                for child in p_noidung.contents:
                    if getattr(child, "name", None) == "table":
                        tables.append(str(child))
                    else:
                        if hasattr(child, "get_text"):
                            noidung += child.get_text(" ", strip=True) + "\n"
                        else:
                            noidung += str(child).strip() + "\n"

            # insert dieu (FK đều là string PK)
            try:
                PDDieu.insert(
                    mapc=mapc,
                    ten=ten,
                    demuc_id=demuc_id,
                    chuong_id=chuong_id,
                    chude_id=chude_id,
                    noidung=noidung.strip(),
                    chimuc=safe_int(dn.get("ChiMuc"), 0),
                    vbqppl=vbqppl,
                    vbqppl_link=vbqppl_link,
                    stt=stt_dieu,
                ).on_conflict_ignore().execute()
            except Exception as e:
                print("❌ FAIL PDDieu:", mapc, "err:", e)
                continue

            # tables
            for t in tables:
                try:
                    PDTable.insert(dieu_id=mapc, html=t).on_conflict_ignore().execute()
                except Exception as e:
                    print("⚠️ FAIL PDTable:", mapc, e)

            # file attachments: thường là các <a href> ngay sau pNoiDung
            if p_noidung:
                sib = p_noidung.find_next_sibling()
                while sib is not None and sib.name == "a":
                    link = sib.get("href")
                    if link:
                        try:
                            PDFile.insert(dieu_id=mapc, link=link, path="").on_conflict_ignore().execute()
                        except Exception as e:
                            print("⚠️ FAIL PDFile:", mapc, link, e)
                    sib = sib.find_next_sibling()

                # liên quan: <p class='pChiDan'>
                if sib is not None and sib.name == "p" and sib.get("class") and sib.get("class")[0] == "pChiDan":
                    for a_lq in sib.select("a[onclick]"):
                        onclick = a_lq.get("onclick", "")
                        if onclick:
                            mapc_lq = extract_input(onclick).replace("'", "")
                            lienquan_pairs.append((mapc, mapc_lq))

            stt_dieu += 1

    # ===== 5) Insert liên quan (chỉ insert khi cả 2 điều tồn tại) =====
    print("Insert pdmuclienquan...")
    for id1, id2 in lienquan_pairs:
        if PDDieu.get_or_none(PDDieu.mapc == id1) is None:
            continue
        if PDDieu.get_or_none(PDDieu.mapc == id2) is None:
            continue
        try:
            PDMucLienQuan.insert(dieu_id1=id1, dieu_id2=id2).on_conflict_ignore().execute()
        except Exception:
            pass

    print("✅ DONE")


if __name__ == "__main__":
    main()
