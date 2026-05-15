from __future__ import annotations

import html
import math
import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "dados" / "GT_NCM_Dados_Brutos_V2.xlsx"
OUT_DIR = ROOT / "analise_descritiva"
W = 1920
H = 1080

GT_COLORS = {
    "GT 1": "#1f8a70",
    "GT 2": "#2c6bed",
    "GT 3": "#d16931",
}


def esc(value: object) -> str:
    return html.escape(str(value or ""))


def gt_key(value: object) -> str:
    match = re.search(r"\bGT\s*\d+\b", str(value or ""), flags=re.IGNORECASE)
    return match.group(0).upper().replace("  ", " ") if match else "GT"


def short(value: object, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def svg_text(x: int, y: int, text: object, size: int, fill: str, weight: int = 400, anchor: str = "start") -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(text)}</text>'


def load_data() -> pd.DataFrame:
    df = pd.read_excel(
        DATA_FILE,
        usecols=["GT", "Subsistema", "Componente", "Criticidade", "Complexidade"],
        dtype={"GT": str, "Subsistema": str, "Componente": str},
    ).rename(
        columns={
            "GT": "gt",
            "Subsistema": "componente",
            "Componente": "subcomponente",
            "Criticidade": "criticidade",
            "Complexidade": "complexidade",
        }
    )
    for col in ["gt", "componente", "subcomponente"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["gt_key"] = df["gt"].map(gt_key)
    df["criticidade"] = pd.to_numeric(df["criticidade"], errors="coerce").fillna(0).astype(int)
    df["complexidade"] = pd.to_numeric(df["complexidade"], errors="coerce").fillna(0).astype(int)
    return df.drop_duplicates(subset=["gt", "componente", "subcomponente"])


def dot_positions(count: int, x: int, y: int, w: int, h: int) -> list[tuple[float, float]]:
    if count <= 0:
        return []
    cols = max(1, math.ceil(math.sqrt(count * w / h)))
    rows = max(1, math.ceil(count / cols))
    gap_x = w / (cols + 1)
    gap_y = h / (rows + 1)
    points = []
    for idx in range(count):
        col = idx % cols
        row = idx // cols
        points.append((x + gap_x * (col + 1), y + gap_y * (row + 1)))
    return points


def build_svg(df: pd.DataFrame) -> str:
    crit_values = [5, 4, 3]
    comp_values = [2, 3, 4]
    x0, y0 = 260, 218
    cell_w, cell_h = 370, 222
    matrix_w, matrix_h = cell_w * len(comp_values), cell_h * len(crit_values)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f7f3ea"/>',
        '<rect width="1920" height="1080" fill="#0f2f3a" opacity="0.035"/>',
        svg_text(70, 84, "Matriz criticidade x complexidade dos subcomponentes", 42, "#172033", 800),
        svg_text(70, 122, "Cada ponto representa um subcomponente; cor indica o GT. Passe o mouse no HTML para ver o nome completo.", 22, "#4e5b6c", 500),
    ]

    # Matrix background.
    parts.append(f'<rect x="{x0}" y="{y0}" width="{matrix_w}" height="{matrix_h}" rx="12" fill="#ffffff" stroke="#d8d3c8"/>')
    for row_idx, crit in enumerate(crit_values):
        y = y0 + row_idx * cell_h
        for col_idx, comp in enumerate(comp_values):
            x = x0 + col_idx * cell_w
            if crit >= 5 and comp >= 4:
                fill = "#fee2d5"
            elif crit >= 4 and comp >= 4:
                fill = "#fff0d8"
            elif crit >= 4:
                fill = "#eef6f2"
            else:
                fill = "#f4f1ea"
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" fill="{fill}" stroke="#ddd4c7"/>')
            cell = df[(df["criticidade"].eq(crit)) & (df["complexidade"].eq(comp))].copy()
            parts.append(svg_text(x + 18, y + 32, f"C{crit} / X{comp}", 20, "#172033", 800))
            parts.append(svg_text(x + cell_w - 18, y + 32, f"{len(cell)}", 23, "#172033", 800, "end"))
            points = dot_positions(len(cell), x + 20, y + 54, cell_w - 40, cell_h - 72)
            cell = cell.sort_values(["gt_key", "subcomponente"])
            for (px, py), item in zip(points, cell.to_dict(orient="records")):
                color = GT_COLORS.get(item["gt_key"], "#455a64")
                title = f"{item['gt']} | {item['componente']} | {item['subcomponente']} | Criticidade {crit} | Complexidade {comp}"
                parts.append(
                    f'<circle cx="{px:.1f}" cy="{py:.1f}" r="7.2" fill="{color}" fill-opacity="0.82" stroke="#ffffff" stroke-width="1.4">'
                    f'<title>{esc(title)}</title></circle>'
                )

    # Axis labels.
    for col_idx, comp in enumerate(comp_values):
        x = x0 + col_idx * cell_w + cell_w / 2
        parts.append(svg_text(int(x), y0 + matrix_h + 46, f"Complexidade {comp}", 19, "#4e5b6c", 800, "middle"))
    for row_idx, crit in enumerate(crit_values):
        y = y0 + row_idx * cell_h + cell_h / 2
        parts.append(svg_text(x0 - 36, int(y + 6), f"Crit. {crit}", 19, "#4e5b6c", 800, "end"))

    # Right panel.
    panel_x = 1430
    parts.append(f'<rect x="{panel_x}" y="218" width="420" height="666" rx="12" fill="#ffffff" stroke="#d8d3c8"/>')
    parts.append(svg_text(panel_x + 28, 260, "Leituras principais", 27, "#172033", 800))
    total = len(df)
    critical_complex = len(df[(df["criticidade"].ge(4)) & (df["complexidade"].ge(4))])
    highest = len(df[(df["criticidade"].eq(5)) & (df["complexidade"].eq(4))])
    parts.append(svg_text(panel_x + 28, 314, str(total), 40, "#172033", 800))
    parts.append(svg_text(panel_x + 108, 314, "subcomponentes mapeados", 17, "#657184", 700))
    parts.append(svg_text(panel_x + 28, 370, str(critical_complex), 40, "#734f96", 800))
    parts.append(svg_text(panel_x + 108, 370, "com alta criticidade/complexidade", 17, "#657184", 700))
    parts.append(svg_text(panel_x + 28, 426, str(highest), 40, "#b24b34", 800))
    parts.append(svg_text(panel_x + 108, 426, "no quadrante mais exigente", 17, "#657184", 700))

    parts.append(svg_text(panel_x + 28, 492, "Distribuição por GT", 22, "#172033", 800))
    for idx, gt in enumerate(["GT 1", "GT 2", "GT 3"]):
        count = len(df[df["gt_key"].eq(gt)])
        y = 528 + idx * 46
        color = GT_COLORS[gt]
        parts.append(f'<circle cx="{panel_x + 42}" cy="{y}" r="9" fill="{color}"/>')
        parts.append(svg_text(panel_x + 62, y + 6, f"{gt}: {count} subcomponentes", 17, "#293447", 800))

    parts.append(svg_text(panel_x + 28, 696, "Como usar", 22, "#172033", 800))
    notes = [
        "C5/X4: maior atenção estratégica.",
        "C4/X4: adensamento técnico relevante.",
        "C4/X2-3: capacidade industrial mais acessível.",
    ]
    for idx, note in enumerate(notes):
        parts.append(svg_text(panel_x + 28, 732 + idx * 34, note, 16, "#596677", 600))

    # Top demanding items.
    top = df.sort_values(["criticidade", "complexidade", "gt_key", "subcomponente"], ascending=[False, False, True, True]).head(8)
    parts.append(svg_text(70, 968, "Exemplos do quadrante de maior exigência:", 18, "#172033", 800))
    x = 70
    for idx, item in enumerate(top.to_dict(orient="records")):
        if idx == 4:
            x = 820
        y = 998 + (idx % 4) * 20
        parts.append(svg_text(x, y, f"{item['gt_key']}: {short(item['subcomponente'], 54)}", 14, "#4e5b6c", 700))

    parts.append("</svg>")
    return "\n".join(parts)


def write_html(svg: str, path: Path) -> None:
    path.write_text(
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Matriz criticidade x complexidade</title>"
        "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
        "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
        f"<body>{svg}</body></html>",
        encoding="utf-8",
    )


def main() -> None:
    df = load_data()
    svg = build_svg(df)
    out_svg = OUT_DIR / "matriz_criticidade_complexidade_subcomponentes.svg"
    out_html = OUT_DIR / "matriz_criticidade_complexidade_subcomponentes.html"
    out_svg.write_text(svg, encoding="utf-8")
    write_html(svg, out_html)
    print(f"OK: {out_svg}")
    print(f"OK: {out_html}")


if __name__ == "__main__":
    main()
