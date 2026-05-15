from __future__ import annotations

import csv
import html
import math
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dados_resumo"
W = 1920
H = 1080

GT_COLORS = {
    "GT 1": "#1f8a70",
    "GT 2": "#2c6bed",
    "GT 3": "#d16931",
}
GT_LABELS = {
    "GT 1": "Baterias",
    "GT 2": "Máquinas elétricas",
    "GT 3": "Inversores/conversores",
}


def read_csv(name: str) -> list[dict]:
    with (DATA / name).open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


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
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def gt_key(value: object) -> str:
    match = re.search(r"\bGT\s*\d+\b", str(value or ""), flags=re.IGNORECASE)
    return match.group(0).upper().replace("  ", " ") if match else "GT"


def text(x: int, y: int, value: object, size: int, fill: str, weight: int = 400, anchor: str = "start") -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(value)}</text>'


def card(x: int, y: int, w: int, h: int, fill: str = "#ffffff") -> str:
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="#d8d3c8"/>'


def base(title: str, subtitle: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f7f3ea"/>',
        '<rect width="1920" height="1080" fill="#0f2f3a" opacity="0.035"/>',
        text(70, 84, title, 42, "#172033", 800),
        text(70, 122, subtitle, 22, "#4e5b6c", 500),
    ]


def svg_end() -> str:
    return "</svg>"


def classify(row: dict) -> str:
    cnpjs = to_int(row["CNPJs unicos"])
    crit = to_int(row["Criticidade"])
    comp = to_int(row["Complexidade"])
    if crit >= 4 and comp >= 4 and cnpjs == 0:
        return "Gap crítico"
    if crit >= 4 and comp >= 4 and cnpjs >= 300:
        return "Adensar agora"
    if cnpjs >= 300 and comp <= 3:
        return "Entrantes"
    if crit >= 4 and comp >= 4:
        return "Desenvolver"
    return "Monitorar"


def slide_a_heatmap() -> str:
    rows = read_csv("componentes.csv")
    parts = base(
        "Prioridades de adensamento por grupo tecnológico",
        "Leitura executiva: onde existe capacidade, onde há gaps e onde entrantes podem acelerar escala",
    )
    parts.append(card(70, 158, 1780, 820))

    statuses = [
        ("Adensar agora", "#1f8a70", "crítico/complexo + muitos CNPJs"),
        ("Gap crítico", "#b24b34", "crítico/complexo + sem CNPJs"),
        ("Desenvolver", "#d16931", "crítico/complexo + poucos CNPJs"),
        ("Entrantes", "#2c6bed", "muitos CNPJs + menor complexidade"),
    ]
    gts = ["GT 1", "GT 2", "GT 3"]
    cell_w, cell_h = 385, 138
    x0, y0 = 360, 290

    parts.append(text(102, 222, "Como interpretar", 25, "#172033", 800))
    for idx, (label, color, desc) in enumerate(statuses):
        y = 258 + idx * 52
        parts.append(f'<circle cx="116" cy="{y}" r="10" fill="{color}"/>')
        parts.append(text(136, y + 6, label, 18, "#172033", 800))
        parts.append(text(136, y + 27, desc, 14, "#657184", 600))

    for col, gt in enumerate(gts):
        x = x0 + col * cell_w
        parts.append(f'<rect x="{x}" y="224" width="330" height="42" rx="10" fill="{GT_COLORS[gt]}"/>')
        parts.append(text(x + 165, 252, f"{gt} | {GT_LABELS[gt]}", 18, "#ffffff", 800, "middle"))

    max_count = 1
    counts = {}
    for gt in gts:
        gt_rows = [row for row in rows if gt_key(row["GT"]) == gt]
        for status, _, _ in statuses:
            count = sum(1 for row in gt_rows if classify(row) == status)
            counts[(gt, status)] = count
            max_count = max(max_count, count)

    for row_idx, (status, color, _) in enumerate(statuses):
        y = y0 + row_idx * cell_h
        parts.append(text(102, y + 78, status, 22, "#172033", 800))
        for col, gt in enumerate(gts):
            x = x0 + col * cell_w
            count = counts[(gt, status)]
            alpha = 0.20 + 0.70 * (count / max_count)
            parts.append(f'<rect x="{x}" y="{y}" width="330" height="104" rx="12" fill="{color}" opacity="{alpha:.2f}"/>')
            parts.append(text(x + 165, y + 57, fmt_int(count), 38, "#172033", 800, "middle"))
            parts.append(text(x + 165, y + 84, "componentes", 15, "#293447", 700, "middle"))

    # Top examples per status.
    parts.append(text(102, 860, "Exemplos para narrar", 24, "#172033", 800))
    examples = {
        "Adensar agora": sorted([r for r in rows if classify(r) == "Adensar agora"], key=lambda r: to_int(r["CNPJs unicos"]), reverse=True)[:3],
        "Gap crítico": sorted([r for r in rows if classify(r) == "Gap crítico"], key=lambda r: (gt_key(r["GT"]), r["Componente"]))[:3],
    }
    x = 102
    for label, ex_rows in examples.items():
        parts.append(text(x, 902, label, 17, "#172033", 800))
        for idx, row in enumerate(ex_rows):
            parts.append(text(x, 930 + idx * 22, f'{gt_key(row["GT"])}: {short(row["Componente"], 38)}', 14, "#4e5b6c", 600))
        x += 620

    parts.append(svg_end())
    return "\n".join(parts)


def slide_b_priority_lanes() -> str:
    rows = read_csv("componentes.csv")
    parts = base(
        "Mapa de ação: onde adensar, desenvolver ou atrair",
        "Lista priorizada por elo, separando capacidade instalada, entrantes potenciais e gaps críticos",
    )
    lanes = [
        ("Adensar agora", "#1f8a70", "Elo crítico/complexo com base empresarial relevante"),
        ("Desenvolver", "#d16931", "Elo crítico/complexo com base empresarial ainda limitada"),
        ("Gap crítico", "#b24b34", "Elo crítico/complexo sem CNPJs associados"),
    ]
    x_positions = [70, 690, 1310]
    for lane_idx, (status, color, desc) in enumerate(lanes):
        x = x_positions[lane_idx]
        parts.append(card(x, 158, 540, 820))
        parts.append(f'<rect x="{x}" y="158" width="540" height="58" rx="10" fill="{color}"/>')
        parts.append(text(x + 26, 196, status, 25, "#ffffff", 800))
        parts.append(text(x + 26, 246, desc, 15, "#657184", 600))
        lane_rows = [row for row in rows if classify(row) == status]
        if status == "Gap crítico":
            lane_rows = sorted(lane_rows, key=lambda row: (gt_key(row["GT"]), row["Componente"]))[:9]
        else:
            lane_rows = sorted(
                lane_rows,
                key=lambda row: (to_int(row["Criticidade"]) + to_int(row["Complexidade"]), to_int(row["CNPJs unicos"])),
                reverse=True,
            )[:9]
        max_cnpj = max([to_int(row["CNPJs unicos"]) for row in lane_rows] + [1])
        for idx, row in enumerate(lane_rows):
            y = 282 + idx * 72
            key = gt_key(row["GT"])
            cnpjs = to_int(row["CNPJs unicos"])
            width = int(250 * cnpjs / max_cnpj)
            parts.append(text(x + 26, y, short(row["Componente"], 44), 16, "#172033", 800))
            parts.append(text(x + 26, y + 22, f'{key} | crit. {row["Criticidade"]} | comp. {row["Complexidade"]}', 13, GT_COLORS.get(key, "#455a64"), 800))
            parts.append(f'<rect x="{x + 246}" y="{y + 8}" width="250" height="13" rx="7" fill="#e8e0d2"/>')
            if width:
                parts.append(f'<rect x="{x + 246}" y="{y + 8}" width="{width}" height="13" rx="7" fill="{GT_COLORS.get(key, color)}"/>')
            parts.append(text(x + 506, y + 20, fmt_int(cnpjs), 14, "#172033", 800, "end"))
            parts.append(text(x + 506, y + 40, "CNPJs", 11, "#657184", 700, "end"))

    parts.append(text(70, 1032, "Uso recomendado: transformar cada coluna em ação estratégica distinta: escalar fornecedores, desenvolver capacidades ou atrair/P&D.", 17, "#596677", 500))
    parts.append(svg_end())
    return "\n".join(parts)


def slide_c_gap_capacity() -> str:
    rows = read_csv("componentes.csv")
    gaps = read_csv("gaps.csv")
    parts = base(
        "Capacidade existente versus lacunas da cadeia",
        "Resumo por GT para comunicar onde há base industrial e onde persistem elos sem cobertura",
    )
    parts.append(card(70, 158, 1780, 820))

    gts = ["GT 1", "GT 2", "GT 3"]
    x0 = 118
    for idx, gt in enumerate(gts):
        x = x0 + idx * 585
        gt_rows = [row for row in rows if gt_key(row["GT"]) == gt]
        sem = sum(1 for row in gt_rows if to_int(row["CNPJs unicos"]) == 0)
        alta = sum(1 for row in gt_rows if to_int(row["Criticidade"]) >= 4 and to_int(row["Complexidade"]) >= 4)
        cap = sum(1 for row in gt_rows if to_int(row["CNPJs unicos"]) >= 300)
        total = len(gt_rows)
        color = GT_COLORS[gt]
        parts.append(f'<rect x="{x}" y="212" width="500" height="58" rx="12" fill="{color}"/>')
        parts.append(text(x + 250, 249, f"{gt} | {GT_LABELS[gt]}", 20, "#ffffff", 800, "middle"))

        metrics = [
            ("componentes", total, "#172033"),
            ("com alta critic./complex.", alta, "#734f96"),
            ("com alta capacidade", cap, "#1f8a70"),
            ("sem CNPJs associados", sem, "#b24b34"),
        ]
        for m_idx, (label, value, m_color) in enumerate(metrics):
            y = 306 + m_idx * 72
            parts.append(text(x, y + 22, fmt_int(value), 34, m_color, 800))
            parts.append(text(x + 84, y + 13, label, 16, "#172033", 800))
            parts.append(f'<rect x="{x + 84}" y="{y + 26}" width="340" height="12" rx="6" fill="#e8e0d2"/>')
            parts.append(f'<rect x="{x + 84}" y="{y + 26}" width="{int(340 * value / max(total, 1))}" height="12" rx="6" fill="{m_color}"/>')

        # Two lists: capabilities and gaps.
        cap_rows = sorted([row for row in gt_rows if to_int(row["CNPJs unicos"]) >= 300], key=lambda row: to_int(row["CNPJs unicos"]), reverse=True)[:4]
        gap_rows = []
        seen = set()
        for row in gaps:
            if gt_key(row["GT"]) == gt and row["Componente"] not in seen:
                seen.add(row["Componente"])
                gap_rows.append(row)
        parts.append(text(x, 632, "Capacidades para adensar", 18, "#1f6f64", 800))
        for r_idx, row in enumerate(cap_rows):
            parts.append(text(x, 662 + r_idx * 24, f'{short(row["Componente"], 42)} ({fmt_int(to_int(row["CNPJs unicos"]))})', 13, "#293447", 700))
        parts.append(text(x, 778, "Gaps para atração/P&D", 18, "#9b3f2e", 800))
        for r_idx, row in enumerate(gap_rows[:4]):
            parts.append(text(x, 808 + r_idx * 24, short(row["Componente"], 46), 13, "#293447", 700))

    parts.append(text(118, 948, "Leitura: os três GTs têm capacidade potencial, mas os gaps se concentram em elos específicos de maior exigência tecnológica.", 18, "#596677", 500))
    parts.append(svg_end())
    return "\n".join(parts)


def write_html(svg: str, path: Path) -> None:
    path.write_text(
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Alternativa slide 2</title>"
        "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
        "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
        f"<body>{svg}</body></html>",
        encoding="utf-8",
    )


def main() -> None:
    slides = [
        ("adensamento_slide_2_alt_a_matriz_gt", slide_a_heatmap()),
        ("adensamento_slide_2_alt_b_mapa_acao", slide_b_priority_lanes()),
        ("adensamento_slide_2_alt_c_gap_capacidade", slide_c_gap_capacity()),
    ]
    for stem, svg in slides:
        svg_path = ROOT / f"{stem}.svg"
        html_path = ROOT / f"{stem}.html"
        svg_path.write_text(svg, encoding="utf-8")
        write_html(svg, html_path)
        print(f"OK: {svg_path}")
        print(f"OK: {html_path}")


if __name__ == "__main__":
    main()
