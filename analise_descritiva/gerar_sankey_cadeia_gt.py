from __future__ import annotations

import html
import math
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "dados" / "GT_NCM_Dados_Brutos_V2.xlsx"
OUT_DIR = ROOT / "analise_descritiva"

W = 1920
H = 1080
PLOT_TOP = 190
PLOT_BOTTOM = 1020
PLOT_H = PLOT_BOTTOM - PLOT_TOP

COLORS = {
    "GT 1": ("#1f8a70", "#d9fff4"),
    "GT 2": ("#2c6bed", "#e7efff"),
    "GT 3": ("#d16931", "#fff0e6"),
}


def esc(value: object) -> str:
    return html.escape(str(value or ""))


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "item"


def gt_key(value: str) -> str:
    match = re.search(r"\bGT\s*\d+\b", value, flags=re.IGNORECASE)
    return match.group(0).upper().replace("  ", " ") if match else "GT"


def normalize_ncm(value: object) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits.zfill(8) if digits else "00000000"


def short_text(value: object, max_len: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def svg_text(x: int, y: int, text: str, size: int, fill: str, weight: int = 400, anchor: str = "start") -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}">{esc(text)}</text>'
    )


def load_data() -> pd.DataFrame:
    df = pd.read_excel(DATA_FILE, dtype=str)
    df = df.rename(
        columns={
            "GT": "gt",
            "Subsistema": "subcomponente",
            "Componente": "componente",
            "Codigo NCM": "ncm",
            "Descricao NCM": "ncm_descricao",
            "Criticidade": "criticidade",
            "Complexidade": "complexidade",
        }
    )
    for col in ["gt", "subcomponente", "componente", "ncm", "ncm_descricao", "criticidade", "complexidade"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["peso_estrategico"] = (
        pd.to_numeric(df["criticidade"], errors="coerce").fillna(0)
        * pd.to_numeric(df["complexidade"], errors="coerce").fillna(0)
    )
    df["ncm_codigo"] = df["ncm"].map(normalize_ncm)
    df["ncm_label"] = df["ncm_codigo"]
    return df.drop_duplicates(subset=["gt", "subcomponente", "componente", "ncm_codigo"])


def numeric_prefix_key(value: object) -> tuple:
    text = str(value or "").strip()
    match = re.match(r"^\s*(\d+(?:\.\d+)*)", text)
    if not match:
        return (999, text)
    parts = tuple(int(part) for part in match.group(1).split("."))
    return (*parts, text)


def make_nodes(
    labels: list[str],
    x: int,
    label_side: str,
    weights: dict[str, int],
    title_lookup: dict[str, str],
    max_label: int,
    sort_mode: str = "weight",
    size_mode: str = "uniform",
) -> list[dict]:
    if sort_mode == "numeric":
        ordered = sorted(labels, key=numeric_prefix_key)
    else:
        ordered = sorted(labels, key=lambda value: (-weights.get(value, 0), value))
    count = len(ordered)
    step = PLOT_H / max(count, 1)
    max_weight = max((weights.get(label, 0) for label in ordered), default=1) or 1
    nodes = []
    for idx, label in enumerate(ordered):
        if size_mode == "weighted":
            node_h = 7 + 30 * math.sqrt(max(weights.get(label, 0), 0) / max_weight)
        else:
            node_h = min(22, max(6, step * 0.58))
        y = PLOT_TOP + step * (idx + 0.5)
        nodes.append(
            {
                "id": label,
                "label": short_text(label, max_label),
                "title": title_lookup.get(label, label),
                "x": x,
                "y": y,
                "h": node_h,
                "side": label_side,
                "weight": weights.get(label, 1),
            }
        )
    return nodes


def draw_node(node: dict, color: str, pale: str, width: int = 10) -> str:
    x = node["x"]
    y = node["y"] - node["h"] / 2
    label_x = x - 12 if node["side"] == "left" else x + width + 12
    anchor = "end" if node["side"] == "left" else "start"
    font = 14 if len(node["label"]) <= 16 else 13
    if len(node["label"]) > 32:
        font = 12
    return "\n".join(
        [
            f'<g><title>{esc(node["title"])}</title>',
            f'<rect x="{x}" y="{y:.2f}" width="{width}" height="{node["h"]:.2f}" rx="3" fill="{color}"/>',
            svg_text(label_x, int(node["y"] + 3), node["label"], font, "#293447", 700, anchor),
            "</g>",
        ]
    )


def link_path(source: dict, target: dict, color: str, width: float, opacity: float) -> str:
    x1 = source["x"] + source.get("w", 12)
    y1 = source["y"]
    x2 = target["x"]
    y2 = target["y"]
    dx = max(80, (x2 - x1) * 0.48)
    return (
        f'<path d="M{x1:.1f},{y1:.1f} C{x1 + dx:.1f},{y1:.1f} {x2 - dx:.1f},{y2:.1f} {x2:.1f},{y2:.1f}" '
        f'fill="none" stroke="{color}" stroke-width="{width:.2f}" stroke-opacity="{opacity:.3f}" stroke-linecap="round"/>'
    )


def column_header(x: int, label: str, color: str, anchor: str = "middle") -> str:
    return "\n".join(
        [
            f'<rect x="{x - 128}" y="148" width="256" height="40" rx="10" fill="{color}"/>',
            svg_text(x, 175, label, 20, "#ffffff", 800, anchor),
        ]
    )


def build_sankey_svg(gt_name: str, part: pd.DataFrame, weighted: bool = False) -> str:
    key = gt_key(gt_name)
    color, pale = COLORS.get(key, ("#455a64", "#eef2f5"))

    ncm_labels = sorted(part["ncm_label"].unique())
    sub_labels = sorted(part["subcomponente"].unique())
    comp_labels = sorted(part["componente"].unique())
    ncm_title = (
        part.sort_values("ncm_label")
        .drop_duplicates("ncm_label")
        .assign(title=lambda data: data["ncm_label"] + " - " + data["ncm_descricao"])
        .set_index("ncm_label")
        ["title"]
        .to_dict()
    )

    weights = defaultdict(int)
    for row in part.itertuples(index=False):
        value = float(row.peso_estrategico) if weighted else 1
        weights[row.ncm_label] += value
        weights[row.subcomponente] += value
        weights[row.componente] += value

    size_mode = "weighted" if weighted else "uniform"
    ncm_nodes = make_nodes(ncm_labels, 170, "left", weights, ncm_title, 14, size_mode=size_mode)
    comp_nodes = make_nodes(comp_labels, 760, "right", weights, {}, 38, sort_mode="numeric", size_mode=size_mode)
    sub_nodes = make_nodes(sub_labels, 1430, "right", weights, {}, 44, sort_mode="numeric", size_mode=size_mode)
    for node in [*ncm_nodes, *comp_nodes, *sub_nodes]:
        node["w"] = 14
    nodes = {node["id"]: node for node in [*ncm_nodes, *sub_nodes, *comp_nodes]}

    links = defaultdict(int)
    for row in part.itertuples(index=False):
        value = float(row.peso_estrategico) if weighted else 1
        links[(row.ncm_label, row.componente)] += value
        links[(row.componente, row.subcomponente)] += value

    max_link = max(links.values() or [1])
    link_parts = []
    for (source, target), value in sorted(links.items(), key=lambda item: (nodes[item[0][0]]["x"], item[0][0], item[0][1])):
        width = 0.9 + (10.0 if weighted else 7.0) * math.sqrt(value / max_link)
        opacity = 0.18 if weighted else (0.16 if nodes[source]["x"] < 1000 else 0.22)
        link_parts.append(link_path(nodes[source], nodes[target], color, width, opacity))

    node_parts = []
    for node in [*ncm_nodes, *comp_nodes, *sub_nodes]:
        node_parts.append(draw_node(node, color, pale, 14))

    if weighted:
        total_weight = int(part["peso_estrategico"].sum())
        subtitle = (
            f"{len(ncm_labels)} NCMs | {len(sub_labels)} subcomponentes | "
            f"{len(comp_labels)} componentes | peso total {total_weight}"
        )
        title = "Sankey ponderado por criticidade x complexidade"
        footer = "Altura dos nós e largura dos fluxos = soma de criticidade x complexidade. Nós maiores indicam maior peso estratégico na cadeia."
    else:
        subtitle = (
            f"{len(ncm_labels)} NCMs | {len(sub_labels)} subcomponentes | "
            f"{len(comp_labels)} componentes | {len(part)} relacoes NCM-componente"
        )
        title = "Sankey da cadeia de componentes"
        footer = "Largura dos fluxos indica quantidade de relacoes. Passe o mouse sobre os NCMs no HTML para ver a descricao."
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
            '<rect width="1920" height="1080" fill="#f6f2ea"/>',
            f'<rect width="1920" height="1080" fill="{pale}" opacity="0.56"/>',
            f'<rect x="0" y="0" width="1920" height="18" fill="{color}"/>',
            svg_text(72, 82, title, 46, "#172033", 800),
            svg_text(72, 124, gt_name, 30, color, 800),
            svg_text(840, 124, subtitle, 20, "#4a5668", 500),
            column_header(170, "NCM", color),
            column_header(760, "Subcomponente", color),
            column_header(1430, "Componente", color),
            *link_parts,
            *node_parts,
            svg_text(72, 1050, footer, 18, "#5e6877", 500),
            "</svg>",
        ]
    )


def write_html(svg: str, out_html: Path) -> None:
    out_html.write_text(
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Sankey da cadeia</title>"
        "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
        "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
        f"<body>{svg}</body></html>",
        encoding="utf-8",
    )


def main() -> None:
    df = load_data()
    outputs = []
    for gt_name, part in df.groupby("gt", sort=True):
        key = slugify(gt_key(gt_name))
        svg = build_sankey_svg(gt_name, part)
        out_svg = OUT_DIR / f"sankey_cadeia_{key}.svg"
        out_html = OUT_DIR / f"sankey_cadeia_{key}.html"
        out_svg.write_text(svg, encoding="utf-8")
        write_html(svg, out_html)
        outputs.append((out_svg, out_html))

        weighted_svg = build_sankey_svg(gt_name, part, weighted=True)
        weighted_out_svg = OUT_DIR / f"sankey_cadeia_ponderado_{key}.svg"
        weighted_out_html = OUT_DIR / f"sankey_cadeia_ponderado_{key}.html"
        weighted_out_svg.write_text(weighted_svg, encoding="utf-8")
        write_html(weighted_svg, weighted_out_html)
        outputs.append((weighted_out_svg, weighted_out_html))

    for svg_path, html_path in outputs:
        print(f"OK: {svg_path}")
        print(f"OK: {html_path}")


if __name__ == "__main__":
    main()
