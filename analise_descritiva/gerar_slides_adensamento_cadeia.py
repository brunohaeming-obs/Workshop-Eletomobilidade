from __future__ import annotations

import csv
import html
import json
import math
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
DATA = ROOT / "dados_resumo"
COMPONENT_INDEX = PROJECT_ROOT / "empresas_app_data" / "componentes"
W = 1920
H = 1080

GT_COLORS = {
    "GT 1": "#1f8a70",
    "GT 2": "#2c6bed",
    "GT 3": "#d16931",
}
GT_PALE = {
    "GT 1": "#d9fff4",
    "GT 2": "#e7efff",
    "GT 3": "#fff0e6",
}


def read_csv(name: str) -> list[dict]:
    with (DATA / name).open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def unique_cnpjs_by_gt() -> dict[str, int]:
    manifest = json.loads((COMPONENT_INDEX / "manifest.json").read_text(encoding="utf-8"))
    by_gt: dict[str, set[str]] = {}
    for component_id in manifest:
        component_manifest = json.loads((COMPONENT_INDEX / component_id / "manifest.json").read_text(encoding="utf-8"))
        columns = component_manifest["columns"]
        index = {name: pos for pos, name in enumerate(columns)}
        for page in range(1, int(component_manifest["pages"]) + 1):
            payload = json.loads((COMPONENT_INDEX / component_id / f"{page}.json").read_text(encoding="utf-8"))
            for row in payload.get("rows", []):
                gt = str(row[index["gt"]])
                cnpj = str(row[index["cnpj"]])
                by_gt.setdefault(gt, set()).add(cnpj)
    return {gt: len(cnpjs) for gt, cnpjs in by_gt.items()}


def to_int(value: object) -> int:
    try:
        return int(float(str(value or "0").replace(".", "").replace(",", ".")))
    except ValueError:
        return 0


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}".replace(",", ".")


def esc(value: object) -> str:
    return html.escape(str(value or ""))


def short(value: object, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def gt_key(value: object) -> str:
    match = re.search(r"\bGT\s*\d+\b", str(value or ""), flags=re.IGNORECASE)
    return match.group(0).upper().replace("  ", " ") if match else "GT"


def svg_text(x: int, y: int, text: object, size: int, fill: str, weight: int = 400, anchor: str = "start") -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(text)}</text>'


def pill(x: int, y: int, w: int, text: str, color: str) -> str:
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="{w}" height="34" rx="9" fill="{color}"/>',
            svg_text(x + w // 2, y + 23, text, 15, "#ffffff", 800, "middle"),
        ]
    )


def draw_card(x: int, y: int, w: int, h: int, fill: str = "#ffffff", stroke: str = "#d8d3c8") -> str:
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}"/>'


def revenue_rank(label: str) -> float:
    low = label.lower()
    if "não informado" in low or "nao informado" in low:
        return -1
    values = []
    for number, unit in re.findall(r"(\d+(?:[\.,]\d+)?)\s*([kmb])?", low):
        amount = float(number.replace(",", "."))
        if unit == "k":
            amount *= 1_000
        elif unit == "m":
            amount *= 1_000_000
        elif unit == "b":
            amount *= 1_000_000_000
        values.append(amount)
    return max(values) if values else -1


def slide_base_industrial() -> str:
    by_gt = sorted(read_csv("by_gt.csv"), key=lambda row: gt_key(row["GT"]))
    cnpjs_unicos_gt = unique_cnpjs_by_gt()
    by_uf = sorted(read_csv("by_uf_chain.csv"), key=lambda row: row["UF"])
    by_revenue = read_csv("by_revenue_chain.csv")
    componentes = read_csv("componentes.csv")

    total_cnpjs = sum(to_int(row["CNPJs"]) for row in by_uf)
    total_municipios = len({(row.get("UF"), row.get("Municipio")) for row in read_csv("by_city_chain.csv")})
    total_componentes = sum(to_int(row["Componentes"]) for row in by_gt)
    top_components = sorted(componentes, key=lambda row: to_int(row["CNPJs unicos"]), reverse=True)[:5]

    revenue_known = [row for row in by_revenue if "informado" not in row["Faixa de faturamento"].lower()]
    grandes = sum(to_int(row["CNPJs"]) for row in revenue_known if revenue_rank(row["Faixa de faturamento"]) >= 100_000_000)
    medias_pequenas = sum(to_int(row["CNPJs"]) for row in revenue_known if 0 <= revenue_rank(row["Faixa de faturamento"]) < 100_000_000)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f7f3ea"/>',
        '<rect width="1920" height="1080" fill="#0f2f3a" opacity="0.035"/>',
        svg_text(70, 84, "Base industrial potencial para adensar a cadeia", 42, "#172033", 800),
        svg_text(70, 122, "Empresas do Sul com capacidades adjacentes para entrar, escalar ou ancorar elos da eletromobilidade", 22, "#4e5b6c", 500),
        draw_card(70, 158, 1780, 108),
    ]

    kpis = [
        ("CNPJs potenciais", total_cnpjs),
        ("municípios com base", total_municipios),
        ("componentes mapeados", total_componentes),
        ("CNPJs em empresas grandes", grandes),
        ("CNPJs médios/pequenos", medias_pequenas),
    ]
    for idx, (label, value) in enumerate(kpis):
        x = 102 + idx * 342
        parts.append(svg_text(x, 208, fmt_int(value), 38, "#172033", 800))
        parts.append(svg_text(x, 238, label, 16, "#657184", 600))

    # Regional board.
    parts.append(draw_card(70, 300, 512, 300))
    parts.append(svg_text(96, 340, "Presença regional equilibrada", 26, "#172033", 800))
    max_uf = max(to_int(row["CNPJs"]) for row in by_uf)
    for idx, row in enumerate(by_uf):
        y = 378 + idx * 64
        cnpjs = to_int(row["CNPJs"])
        width = int(300 * cnpjs / max_uf)
        parts.append(svg_text(96, y + 25, row["UF"], 24, "#172033", 800))
        parts.append(f'<rect x="154" y="{y + 7}" width="300" height="18" rx="9" fill="#e8e0d2"/>')
        parts.append(f'<rect x="154" y="{y + 7}" width="{width}" height="18" rx="9" fill="#2e7d73"/>')
        parts.append(svg_text(468, y + 25, fmt_int(cnpjs), 20, "#172033", 800, "end"))
        parts.append(svg_text(468, y + 47, f'{fmt_int(to_int(row["Municipios"]))} municípios', 13, "#657184", 600, "end"))
    parts.append(svg_text(96, 566, "A base aparece nos três estados, reduzindo dependência de um único polo.", 15, "#596677", 500))

    # GT columns.
    parts.append(draw_card(610, 300, 620, 300))
    parts.append(svg_text(636, 340, "Capacidade por grupo tecnológico", 26, "#172033", 800))
    for idx, row in enumerate(by_gt):
        key = gt_key(row["GT"])
        x = 636 + idx * 195
        color = GT_COLORS.get(key, "#455a64")
        parts.append(pill(x, 368, 166, key, color))
        parts.append(svg_text(x, 426, short(row["GT"].split("–", 1)[-1].strip(), 22), 16, "#172033", 800))
        parts.append(svg_text(x, 462, fmt_int(cnpjs_unicos_gt.get(row["GT"], 0)), 32, color, 800))
        parts.append(svg_text(x, 486, "CNPJs únicos", 14, "#657184", 600))
        parts.append(svg_text(x, 522, f'{fmt_int(to_int(row["Componentes"]))} comp. | {fmt_int(to_int(row["NCMs"]))} NCMs', 14, "#293447", 700))
        parts.append(svg_text(x, 548, f'{fmt_int(to_int(row["Patentes"]))} patentes', 14, "#293447", 700))

    # Revenue composition.
    parts.append(draw_card(1260, 300, 590, 300))
    parts.append(svg_text(1286, 340, "Base estabelecida e entrantes potenciais", 26, "#172033", 800))
    total_known = max(grandes + medias_pequenas, 1)
    big_w = int(500 * grandes / total_known)
    mid_w = 500 - big_w
    parts.append(f'<rect x="1288" y="382" width="{mid_w}" height="42" rx="12" fill="#64a999"/>')
    parts.append(f'<rect x="{1288 + mid_w}" y="382" width="{big_w}" height="42" rx="12" fill="#16324f"/>')
    parts.append(svg_text(1288, 460, fmt_int(medias_pequenas), 34, "#1f6f64", 800))
    parts.append(svg_text(1288, 486, "CNPJs médios/pequenos: pool de entrantes", 15, "#657184", 600))
    parts.append(svg_text(1588, 460, fmt_int(grandes), 34, "#16324f", 800))
    parts.append(svg_text(1588, 486, "CNPJs em faixas >= 100M: âncoras", 15, "#657184", 600))
    parts.append(svg_text(1288, 540, "Leitura: existe volume para entrada/reconversão e empresas de porte para puxar escala.", 16, "#293447", 600))

    # Top elos.
    parts.append(draw_card(70, 636, 1780, 350))
    parts.append(svg_text(96, 676, "Elos com maior massa empresarial potencial", 27, "#172033", 800))
    max_comp = max(to_int(row["CNPJs unicos"]) for row in top_components)
    for idx, row in enumerate(top_components):
        y = 718 + idx * 48
        color = GT_COLORS.get(gt_key(row["GT"]), "#455a64")
        width = int(720 * to_int(row["CNPJs unicos"]) / max_comp)
        parts.append(svg_text(96, y + 20, short(row["Componente"], 42), 17, "#172033", 800))
        parts.append(svg_text(470, y + 20, gt_key(row["GT"]), 14, color, 800))
        parts.append(f'<rect x="540" y="{y + 4}" width="720" height="20" rx="10" fill="#e8e0d2"/>')
        parts.append(f'<rect x="540" y="{y + 4}" width="{width}" height="20" rx="10" fill="{color}"/>')
        parts.append(svg_text(1284, y + 21, fmt_int(to_int(row["CNPJs unicos"])), 18, "#172033", 800))
        parts.append(svg_text(1370, y + 21, f'{fmt_int(to_int(row["CNPJs com patente"]))} CNPJs com patente', 16, "#596677", 600))
    parts.append(svg_text(96, 956, "Esses elos indicam onde há capacidade industrial ampla para adensamento rápido ou novos entrantes adjacentes.", 17, "#596677", 500))
    parts.append("</svg>")
    return "\n".join(parts)


def slide_prioridades() -> str:
    componentes = read_csv("componentes.csv")
    gaps = read_csv("gaps.csv")
    risk = read_csv("risk_matrix.csv")

    max_cnpj = max(to_int(row["CNPJs unicos"]) for row in componentes)
    max_pat = max(to_int(row["CNPJs com patente"]) for row in componentes)
    high_capacity = [
        row
        for row in componentes
        if to_int(row["Criticidade"]) >= 4 and to_int(row["Complexidade"]) >= 4 and to_int(row["CNPJs unicos"]) >= 300
    ]
    strategic_gaps = [
        row
        for row in componentes
        if to_int(row["Criticidade"]) >= 4 and to_int(row["Complexidade"]) >= 4 and to_int(row["CNPJs unicos"]) == 0
    ]
    entrants = [
        row
        for row in componentes
        if to_int(row["CNPJs unicos"]) >= 300 and to_int(row["Complexidade"]) <= 3
    ]
    labels = sorted(high_capacity, key=lambda row: to_int(row["CNPJs unicos"]), reverse=True)[:3]
    labels += sorted(strategic_gaps, key=lambda row: (to_int(row["Criticidade"]), to_int(row["Complexidade"])), reverse=True)[:3]
    label_ids = {row["Componente"] for row in labels}

    x0, y0 = 110, 220
    plot_w, plot_h = 1020, 690
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f7f3ea"/>',
        '<rect width="1920" height="1080" fill="#0f2f3a" opacity="0.035"/>',
        svg_text(70, 84, "Onde adensar: capacidades, gaps e complexidade", 42, "#172033", 800),
        svg_text(70, 122, "Cada ponto é um componente; posição mostra massa empresarial e criticidade/complexidade", 22, "#4e5b6c", 500),
        draw_card(70, 158, 1120, 820),
    ]

    # Axes and quadrants.
    parts.append(f'<rect x="{x0}" y="{y0}" width="{plot_w}" height="{plot_h}" rx="10" fill="#ffffff" stroke="#d8d3c8"/>')
    mid_x = x0 + int(plot_w * 0.52)
    mid_y = y0 + int(plot_h * 0.48)
    parts.append(f'<rect x="{x0}" y="{y0}" width="{mid_x-x0}" height="{mid_y-y0}" fill="#fee8df" opacity="0.65"/>')
    parts.append(f'<rect x="{mid_x}" y="{y0}" width="{x0+plot_w-mid_x}" height="{mid_y-y0}" fill="#e3f7ee" opacity="0.80"/>')
    parts.append(f'<rect x="{x0}" y="{mid_y}" width="{plot_w}" height="{y0+plot_h-mid_y}" fill="#eef2f7" opacity="0.75"/>')
    parts.append(f'<line x1="{mid_x}" y1="{y0}" x2="{mid_x}" y2="{y0+plot_h}" stroke="#c9c1b4" stroke-dasharray="8 8"/>')
    parts.append(f'<line x1="{x0}" y1="{mid_y}" x2="{x0+plot_w}" y2="{mid_y}" stroke="#c9c1b4" stroke-dasharray="8 8"/>')
    parts.append(svg_text(x0 + 26, y0 + 34, "Gaps estratégicos", 20, "#9b3f2e", 800))
    parts.append(svg_text(mid_x + 26, y0 + 34, "Capacidade para adensar", 20, "#1f6f64", 800))
    parts.append(svg_text(mid_x + 26, y0 + plot_h - 28, "Entrantes potenciais", 20, "#345a92", 800))
    parts.append(svg_text(x0 + plot_w // 2, y0 + plot_h + 48, "CNPJs únicos associados ao elo", 18, "#4e5b6c", 700, "middle"))
    parts.append(svg_text(x0 - 46, y0 + plot_h // 2, "criticidade + complexidade", 18, "#4e5b6c", 700, "middle"))

    # Dots.
    for row in componentes:
        cnpj = to_int(row["CNPJs unicos"])
        score = to_int(row["Criticidade"]) + to_int(row["Complexidade"])
        x = x0 + 36 + int((plot_w - 72) * math.sqrt(cnpj / max(max_cnpj, 1)))
        y = y0 + plot_h - 48 - int((plot_h - 96) * ((score - 5) / 4))
        patents = to_int(row["CNPJs com patente"])
        radius = 5 + 13 * math.sqrt(patents / max(max_pat, 1))
        color = GT_COLORS.get(gt_key(row["GT"]), "#455a64")
        parts.append(f'<circle cx="{x}" cy="{y}" r="{radius:.1f}" fill="{color}" fill-opacity="0.64" stroke="#ffffff" stroke-width="1.5"><title>{esc(row["Componente"])} | {fmt_int(cnpj)} CNPJs | crit. {row["Criticidade"]} comp. {row["Complexidade"]}</title></circle>')
        if row["Componente"] in label_ids:
            lx = min(x + 12, x0 + plot_w - 220)
            parts.append(svg_text(lx, y - 10, short(row["Componente"], 30), 11, "#172033", 800))

    # Legend cards.
    parts.append(draw_card(1220, 158, 630, 252))
    parts.append(svg_text(1248, 200, "Resumo dos quadrantes", 26, "#172033", 800))
    summary = [
        ("Capacidade instalada", len(high_capacity), "#1f8a70", "críticos/complexos com muitos CNPJs"),
        ("Gaps estratégicos", len(strategic_gaps), "#b24b34", "críticos/complexos sem CNPJs"),
        ("Entrantes potenciais", len(entrants), "#2c6bed", "muitos CNPJs e menor complexidade"),
    ]
    for idx, (label, value, color, desc) in enumerate(summary):
        y = 232 + idx * 52
        parts.append(f'<circle cx="1260" cy="{y}" r="10" fill="{color}"/>')
        parts.append(svg_text(1282, y + 6, f"{fmt_int(value)} {label.lower()}", 18, "#172033", 800))
        parts.append(svg_text(1282, y + 26, desc, 14, "#657184", 600))

    parts.append(draw_card(1220, 438, 630, 252))
    parts.append(svg_text(1248, 480, "Matriz criticidade x complexidade", 25, "#172033", 800))
    max_risk = max(to_int(row["Componentes"]) for row in risk)
    for idx, row in enumerate(sorted(risk, key=lambda item: (to_int(item["Criticidade"]), to_int(item["Complexidade"])), reverse=True)):
        y = 516 + idx * 27
        width = int(230 * to_int(row["Componentes"]) / max_risk)
        parts.append(svg_text(1248, y + 15, f'C{row["Criticidade"]} / X{row["Complexidade"]}', 14, "#293447", 800))
        parts.append(f'<rect x="1340" y="{y + 3}" width="230" height="14" rx="7" fill="#e8e0d2"/>')
        parts.append(f'<rect x="1340" y="{y + 3}" width="{width}" height="14" rx="7" fill="#734f96"/>')
        parts.append(svg_text(1584, y + 16, fmt_int(to_int(row["Componentes"])), 14, "#172033", 800))
        parts.append(svg_text(1630, y + 16, f'{fmt_int(to_int(row["CNPJs"]))} CNPJs', 13, "#657184", 600))

    parts.append(draw_card(1220, 718, 630, 260))
    parts.append(svg_text(1248, 760, "Gaps prioritários para atração/desenvolvimento", 24, "#172033", 800))
    gap_components = []
    seen = set()
    for row in gaps:
        key = row["Componente"]
        if key not in seen:
            seen.add(key)
            gap_components.append(row)
    for idx, row in enumerate(gap_components[:6]):
        y = 792 + idx * 28
        parts.append(svg_text(1248, y + 15, short(row["Componente"], 48), 14, "#172033", 800))
        parts.append(svg_text(1668, y + 15, gt_key(row["GT"]), 13, GT_COLORS.get(gt_key(row["GT"]), "#455a64"), 800))
    parts.append(svg_text(1248, 956, "Leitura: priorizar elos no alto da matriz; quando estão à esquerda, exigem atração ou P&D.", 15, "#596677", 500))

    # GT legend.
    for idx, (key, color) in enumerate(GT_COLORS.items()):
        x = 132 + idx * 150
        parts.append(f'<circle cx="{x}" cy="944" r="9" fill="{color}"/>')
        parts.append(svg_text(x + 16, 949, key, 14, "#293447", 800))
    parts.append("</svg>")
    return "\n".join(parts)


def slide_gt_detail(gt_name: str) -> str:
    by_gt = {row["GT"]: row for row in read_csv("by_gt.csv")}
    componentes = [row for row in read_csv("componentes.csv") if row["GT"] == gt_name]
    gaps = [row for row in read_csv("gaps.csv") if row["GT"] == gt_name]
    cnpjs_unicos_gt = unique_cnpjs_by_gt()
    key = gt_key(gt_name)
    color = GT_COLORS.get(key, "#455a64")
    pale = {
        "GT 1": "#d9fff4",
        "GT 2": "#e7efff",
        "GT 3": "#fff0e6",
    }.get(key, "#eef2f5")
    row_gt = by_gt[gt_name]

    high_capacity = sorted(componentes, key=lambda row: to_int(row["CNPJs unicos"]), reverse=True)[:7]
    critical = sorted(
        [row for row in componentes if to_int(row["Criticidade"]) >= 4 and to_int(row["Complexidade"]) >= 4],
        key=lambda row: (to_int(row["CNPJs unicos"]), to_int(row["CNPJs com patente"])),
        reverse=True,
    )[:6]
    gap_components = []
    seen = set()
    for row in gaps:
        if row["Componente"] not in seen:
            seen.add(row["Componente"])
            gap_components.append(row)

    max_capacity = max([to_int(row["CNPJs unicos"]) for row in high_capacity] + [1])
    max_patents = max([to_int(row["CNPJs com patente"]) for row in componentes] + [1])
    total_components = len(componentes)
    high_cap_count = sum(1 for row in componentes if to_int(row["CNPJs unicos"]) >= 300)
    critical_count = sum(1 for row in componentes if to_int(row["Criticidade"]) >= 4 and to_int(row["Complexidade"]) >= 4)
    gap_count = sum(1 for row in componentes if to_int(row["CNPJs unicos"]) == 0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f7f3ea"/>',
        f'<rect width="1920" height="1080" fill="{pale}" opacity="0.52"/>',
        f'<rect x="0" y="0" width="1920" height="18" fill="{color}"/>',
        svg_text(70, 84, "Base industrial por grupo tecnológico", 42, "#172033", 800),
        svg_text(70, 124, gt_name, 28, color, 800),
        svg_text(70, 156, "Onde há massa empresarial para adensar e onde estão os elos críticos para desenvolver ou atrair", 20, "#4e5b6c", 500),
        draw_card(70, 188, 1780, 116),
    ]

    kpis = [
        ("CNPJs únicos", cnpjs_unicos_gt.get(gt_name, 0)),
        ("componentes", total_components),
        ("NCMs", to_int(row_gt["NCMs"])),
        ("municípios", to_int(row_gt["Municipios"])),
        ("patentes", to_int(row_gt["Patentes"])),
    ]
    for idx, (label, value) in enumerate(kpis):
        x = 106 + idx * 342
        parts.append(svg_text(x, 240, fmt_int(value), 38, "#172033", 800))
        parts.append(svg_text(x, 270, label, 16, "#657184", 600))

    # Strategic mix.
    parts.append(draw_card(820, 338, 390, 250))
    parts.append(svg_text(846, 378, "Leitura do GT", 25, "#172033", 800))
    mix = [
        ("Capacidade alta", high_cap_count, "#1f8a70"),
        ("Críticos/complexos", critical_count, "#734f96"),
        ("Gaps sem CNPJs", gap_count, "#b24b34"),
    ]
    for idx, (label, value, m_color) in enumerate(mix):
        y = 420 + idx * 48
        width = int(310 * value / max(total_components, 1))
        width = int(210 * value / max(total_components, 1))
        parts.append(svg_text(846, y + 15, fmt_int(value), 25, m_color, 800))
        parts.append(svg_text(908, y + 13, label, 15, "#293447", 800))
        parts.append(f'<rect x="908" y="{y + 24}" width="210" height="11" rx="6" fill="#e8e0d2"/>')
        parts.append(f'<rect x="908" y="{y + 24}" width="{width}" height="11" rx="6" fill="{m_color}"/>')
    parts.append(svg_text(846, 564, "Separar escala, desenvolvimento e atração.", 13, "#596677", 500))

    # Top capacity list.
    parts.append(draw_card(820, 622, 390, 310))
    parts.append(svg_text(846, 662, "Elos com maior capacidade", 23, "#172033", 800))
    for idx, row in enumerate(high_capacity[:4]):
        y = 700 + idx * 52
        cnpjs = to_int(row["CNPJs unicos"])
        width = int(205 * cnpjs / max_capacity)
        parts.append(svg_text(846, y, short(row["Componente"], 36), 14, "#172033", 800))
        parts.append(f'<rect x="846" y="{y + 15}" width="205" height="12" rx="6" fill="#e8e0d2"/>')
        parts.append(f'<rect x="846" y="{y + 15}" width="{width}" height="12" rx="6" fill="{color}"/>')
        parts.append(svg_text(1068, y + 27, fmt_int(cnpjs), 13, "#172033", 800))
        parts.append(svg_text(1132, y + 27, "CNPJs", 11, "#657184", 700))

    # Critical + gaps.
    parts.append(draw_card(1260, 338, 590, 594))
    parts.append(svg_text(1286, 378, "Prioridades de adensamento", 26, "#172033", 800))
    parts.append(svg_text(1286, 416, "Críticos/complexos com capacidade", 17, "#1f6f64", 800))
    for idx, row in enumerate(critical[:5]):
        y = 448 + idx * 32
        parts.append(svg_text(1286, y, short(row["Componente"], 47), 14, "#172033", 800))
        parts.append(svg_text(1688, y, f'{fmt_int(to_int(row["CNPJs unicos"]))} CNPJs', 13, "#657184", 700))
    parts.append(svg_text(1286, 636, "Gaps para atração, P&D ou novos fornecedores", 17, "#9b3f2e", 800))
    if gap_components:
        for idx, row in enumerate(gap_components[:7]):
            y = 668 + idx * 30
            parts.append(svg_text(1286, y, short(row["Componente"], 52), 14, "#172033", 800))
            parts.append(svg_text(1710, y, short(row["NCM"], 10), 12, "#657184", 700))
    else:
        parts.append(svg_text(1286, 674, "Sem gaps zerados mapeados neste GT.", 15, "#657184", 600))

    parts.append(svg_text(70, 1028, "Nota: CNPJs por GT são únicos dentro do GT; a mesma empresa pode aparecer em mais de um GT.", 16, "#596677", 500))
    parts.append("</svg>")
    return "\n".join(parts)


def write_html(svg: str, path: Path) -> None:
    path.write_text(
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Adensamento da cadeia</title>"
        "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
        "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
        f"<body>{svg}</body></html>",
        encoding="utf-8",
    )


def main() -> None:
    slides = [
        ("adensamento_slide_1_base_industrial", slide_base_industrial()),
        ("adensamento_slide_2_prioridades", slide_prioridades()),
    ]
    for gt_name in sorted(unique_cnpjs_by_gt(), key=gt_key):
        slides.append((f"adensamento_slide_2_{gt_key(gt_name).lower().replace(' ', '_')}", slide_gt_detail(gt_name)))
    for stem, svg in slides:
        svg_path = ROOT / f"{stem}.svg"
        html_path = ROOT / f"{stem}.html"
        svg_path.write_text(svg, encoding="utf-8")
        write_html(svg, html_path)
        print(f"OK: {svg_path}")
        print(f"OK: {html_path}")


if __name__ == "__main__":
    main()
