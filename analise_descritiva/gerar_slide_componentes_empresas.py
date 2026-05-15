from __future__ import annotations

import csv
import html
import json
import re
import textwrap
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "empresas_app_data" / "componentes"
COMPONENTES_XLSX_V2 = ROOT / "dados" / "GT_NCM_Dados_Brutos_V2.xlsx"
OUT_DIR = ROOT / "analise_descritiva"
OUT_CSV = OUT_DIR / "dados_resumo" / "componentes_empresas_destaques.csv"
OUT_SVG = OUT_DIR / "slide_componentes_empresas.svg"
OUT_HTML = OUT_DIR / "slide_componentes_empresas.html"

W = 1920
H = 1080
GT_PAGE_SIZE = 15
NA_VALUE = "Nao informado"

COLORS = {
    "GT 1": ("#1f8a70", "#d9fff4"),
    "GT 2": ("#2c6bed", "#e7efff"),
    "GT 3": ("#d16931", "#fff0e6"),
}

COMPONENT_CACHE: dict[str, dict] = {}


def normalize_text(value: object) -> str:
    text = str(value or "")
    replacements = {
        "Ãº": "u",
        "Ã£": "a",
        "Ã§": "c",
        "Ã©": "e",
        "Ãª": "e",
        "Ã¡": "a",
        "Ã³": "o",
        "Ã­": "i",
        "â€“": "-",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text or "item"


def load_component_metadata() -> dict[str, dict]:
    componentes = pd.read_excel(
        COMPONENTES_XLSX_V2,
        usecols=["GT", "Subsistema", "Componente"],
        dtype={"GT": str, "Subsistema": str, "Componente": str},
    ).rename(columns={"GT": "gt", "Subsistema": "subsistema", "Componente": "componente"})
    for col in ["gt", "subsistema", "componente"]:
        componentes[col] = componentes[col].fillna(NA_VALUE).astype(str).str.strip()

    ids = []
    used: dict[str, int] = {}
    for row in componentes[["gt", "subsistema", "componente"]].drop_duplicates().itertuples(index=False):
        base = slugify(f"{row.gt}-{row.subsistema}-{row.componente}")[:72].strip("-")
        count = used.get(base, 0) + 1
        used[base] = count
        ids.append(base if count == 1 else f"{base}-{count}")

    unique = componentes[["gt", "subsistema", "componente"]].drop_duplicates().copy()
    unique["component_id"] = ids
    return unique.set_index("component_id").to_dict(orient="index")


def revenue_rank(value: object) -> float:
    label = normalize_text(value).lower()
    if not label or "nao informado" in label:
        return -1.0
    values = []
    for number, unit in re.findall(r"(\d+(?:[\.,]\d+)?)\s*([kmb])?", label):
        amount = float(number.replace(",", "."))
        if unit == "k":
            amount *= 1_000
        elif unit == "m":
            amount *= 1_000_000
        elif unit == "b":
            amount *= 1_000_000_000
        values.append(amount)
    if not values:
        return -1.0
    value = max(values)
    return value + 1 if "superior" in label else value


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}".replace(",", ".")


def fmt_revenue(value: object) -> str:
    label = normalize_text(value).strip()
    return label if label else NA_VALUE


def short_company(value: object, max_len: int = 34) -> str:
    text = normalize_text(value).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(
        r"\b(S\.?A\.?|LTDA\.?|INDUSTRIA|COMERCIO|DE|DA|DO|DAS|DOS|E)\b",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s+", " ", text).strip(" -.,")
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def short_component(value: object) -> str:
    text = normalize_text(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def load_component(component_id: str) -> dict:
    if component_id in COMPONENT_CACHE:
        return COMPONENT_CACHE[component_id]
    manifest = json.loads((DATA_DIR / component_id / "manifest.json").read_text(encoding="utf-8"))
    columns = manifest["columns"]
    rows = []
    for page in range(1, int(manifest["pages"]) + 1):
        payload = json.loads((DATA_DIR / component_id / f"{page}.json").read_text(encoding="utf-8"))
        rows.extend(dict(zip(columns, row)) for row in payload.get("rows", []))
    COMPONENT_CACHE[component_id] = {"columns": columns, "rows": rows}
    return COMPONENT_CACHE[component_id]


def top_names(rows: list[dict], mode: str) -> str:
    if mode == "revenue":
        ranked = sorted(
            rows,
            key=lambda row: (
                revenue_rank(row.get("faturamento")),
                int(row.get("patentes") or 0),
                normalize_text(row.get("razao")),
            ),
            reverse=True,
        )
    else:
        ranked = sorted(
            rows,
            key=lambda row: (
                int(row.get("patentes") or 0),
                revenue_rank(row.get("faturamento")),
                normalize_text(row.get("razao")),
            ),
            reverse=True,
        )
    items = []
    seen = set()
    for row in ranked:
        cnpj = str(row.get("cnpj", ""))
        if cnpj in seen:
            continue
        seen.add(cnpj)
        name = short_company(row.get("razao"))
        if mode == "revenue":
            suffix = fmt_revenue(row.get("faturamento"))
            items.append(f"{name} ({suffix})")
        else:
            items.append(name)
        if len(items) == 2:
            break
    return " | ".join(items) if items else "-"


def build_data() -> list[dict]:
    manifest = json.loads((DATA_DIR / "manifest.json").read_text(encoding="utf-8"))
    metadata = load_component_metadata()
    output = []
    for component_id in manifest:
        payload = load_component(component_id)
        rows = payload["rows"]
        first = rows[0] if rows else {}
        meta = metadata.get(component_id, {})
        gt = normalize_text(first.get("gt") or meta.get("gt") or component_id.split("-")[0].upper())
        gt_match = re.search(r"\bGT\s*\d+\b", gt, flags=re.IGNORECASE)
        gt_key = gt_match.group(0).upper().replace("  ", " ") if gt_match else "GT"
        subsistema = normalize_text(first.get("subsistema") or meta.get("subsistema") or "Sem subsistema")
        componente = normalize_text(first.get("componente") or meta.get("componente") or component_id)
        unique_cnpjs = len({str(row.get("cnpj", "")) for row in rows if row.get("cnpj")})
        output.append(
            {
                "component_id": component_id,
                "gt": gt,
                "gt_key": gt_key,
                "subsistema": subsistema,
                "componente": componente,
                "cnpjs_unicos": unique_cnpjs,
                "destaques_faturamento": top_names(rows, "revenue"),
                "destaques_patentes": top_names(rows, "patents"),
            }
        )
    return sorted(output, key=lambda row: (row["gt"], -row["cnpjs_unicos"], row["componente"]))


def wrap_lines(text: str, width: int, max_lines: int) -> list[str]:
    lines = textwrap.wrap(text, width=width, break_long_words=False, replace_whitespace=False)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(". ") + "..."
    return lines or [""]


def svg_text(x: int, y: int, text: str, size: int, fill: str, weight: int = 400) -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}">{html.escape(text)}</text>'


def svg_multiline(x: int, y: int, lines: list[str], size: int, fill: str, weight: int = 400, gap: int = 22) -> str:
    return "\n".join(svg_text(x, y + idx * gap, line, size, fill, weight) for idx, line in enumerate(lines))


def build_svg(rows: list[dict]) -> str:
    nonzero = [row for row in rows if row["cnpjs_unicos"] > 0]
    total_components = len(rows)
    active_components = len(nonzero)
    total_unique = len(
        {
            str(company.get("cnpj", ""))
            for row in rows
            for company in load_component(row["component_id"])["rows"]
            if company.get("cnpj")
        }
    )
    groups: dict[str, list[dict]] = {}
    for row in nonzero:
        groups.setdefault(row["gt_key"], []).append(row)
    ordered_keys = sorted(groups)
    selected = {key: sorted(groups[key], key=lambda row: row["cnpjs_unicos"], reverse=True)[:6] for key in ordered_keys}
    max_count = max((row["cnpjs_unicos"] for items in selected.values() for row in items), default=1)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f6f2ea"/>',
        '<rect x="0" y="0" width="1920" height="1080" fill="#10263d" opacity="0.05"/>',
        svg_text(72, 92, "Mapa de empresas por componente", 48, "#172033", 800),
        svg_text(72, 128, "CNPJs unicos e destaques por maior faturamento e volume de patentes", 23, "#4a5668", 500),
    ]

    kpis = [
        ("CNPJs unicos", total_unique),
        ("componentes com empresas", active_components),
        ("componentes mapeados", total_components),
    ]
    kpi_x = 1110
    for idx, (label, value) in enumerate(kpis):
        x = kpi_x + idx * 240
        parts.append(f'<rect x="{x}" y="58" width="208" height="94" rx="8" fill="#ffffff" stroke="#d8d3c8"/>')
        parts.append(svg_text(x + 20, 99, fmt_int(value), 36, "#172033", 800))
        parts.append(svg_text(x + 20, 128, label, 17, "#5e6877", 500))

    col_w = 568
    left = 72
    top = 188
    gap = 34
    card_h = 126
    card_gap = 16
    for col_idx, key in enumerate(ordered_keys[:3]):
        x = left + col_idx * (col_w + gap)
        color, pale = COLORS.get(key, ("#455a64", "#eef2f5"))
        display_gt = selected[key][0]["gt"] if selected.get(key) else key
        parts.append(f'<rect x="{x}" y="{top - 44}" width="{col_w}" height="34" rx="8" fill="{color}"/>')
        parts.append(svg_text(x + 18, top - 20, display_gt, 20, "#ffffff", 800))
        parts.append(svg_text(x + col_w - 158, top - 20, f"top {len(selected[key])} comp.", 17, "#ffffff", 500))

        for row_idx, row in enumerate(selected[key]):
            y = top + row_idx * (card_h + card_gap)
            count = row["cnpjs_unicos"]
            bar_w = max(36, int((count / max_count) * 194))
            parts.append(f'<rect x="{x}" y="{y}" width="{col_w}" height="{card_h}" rx="8" fill="#ffffff" stroke="#d8d3c8"/>')
            parts.append(f'<rect x="{x}" y="{y}" width="9" height="{card_h}" rx="4" fill="{color}"/>')
            parts.append(f'<rect x="{x + col_w - 232}" y="{y + 18}" width="194" height="16" rx="8" fill="{pale}"/>')
            parts.append(f'<rect x="{x + col_w - 232}" y="{y + 18}" width="{bar_w}" height="16" rx="8" fill="{color}"/>')
            parts.append(svg_text(x + col_w - 104, y + 73, fmt_int(count), 44, color, 800))
            parts.append(svg_text(x + col_w - 110, y + 98, "CNPJs", 16, "#687386", 600))
            parts.append(svg_multiline(x + 24, y + 35, wrap_lines(short_component(row["componente"]), 38, 2), 20, "#1c2637", 800, 24))
            parts.append(svg_text(x + 24, y + 86, "Faturamento", 14, "#687386", 800))
            parts.append(svg_text(x + 122, y + 86, html.unescape(row["destaques_faturamento"])[:68], 14, "#283346", 500))
            parts.append(svg_text(x + 24, y + 111, "Patentes", 14, "#687386", 800))
            parts.append(svg_text(x + 94, y + 111, html.unescape(row["destaques_patentes"])[:78], 14, "#283346", 500))

    parts.append(svg_text(72, 1034, "Nota: destaques por faturamento usam a maior faixa informada; patentes usam total de pedidos associados ao CNPJ na base INPI.", 17, "#5e6877", 500))
    parts.append("</svg>")
    return "\n".join(parts)


def split_columns(items: list[dict], columns: int) -> list[list[dict]]:
    size = max(1, (len(items) + columns - 1) // columns)
    return [items[index : index + size] for index in range(0, len(items), size)]


def metric_text(row: dict) -> str:
    count = row["cnpjs_unicos"]
    return "0" if count == 0 else fmt_int(count)


def highlight_lines(value: str, width: int = 50, max_lines: int = 2) -> list[str]:
    if not value or value == "-":
        return ["-"]
    items = [item.strip() for item in value.split("|") if item.strip()]
    lines = []
    for idx, item in enumerate(items[:max_lines], start=1):
        wrapped = wrap_lines(f"{idx}. {item}", width, 1)[0]
        lines.append(wrapped)
    return lines or ["-"]


def build_gt_svg(gt_key: str, rows: list[dict], page_rows: list[dict] | None = None, page: int = 1, pages: int = 1) -> str:
    gt_rows = sorted(
        [row for row in rows if row["gt_key"] == gt_key],
        key=lambda row: (row["subsistema"], row["componente"]),
    )
    visible_rows = page_rows if page_rows is not None else gt_rows
    color, pale = COLORS.get(gt_key, ("#455a64", "#eef2f5"))
    display_gt = gt_rows[0]["gt"] if gt_rows else gt_key
    active = [row for row in gt_rows if row["cnpjs_unicos"] > 0]
    total_unique = len(
        {
            str(company.get("cnpj", ""))
            for row in gt_rows
            for company in load_component(row["component_id"])["rows"]
            if company.get("cnpj")
        }
    )
    max_count = max((row["cnpjs_unicos"] for row in gt_rows), default=1)
    columns = split_columns(visible_rows, 3)
    max_rows = max((len(col_rows) for col_rows in columns), default=1)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<rect width="1920" height="1080" fill="#f6f2ea"/>',
        f'<rect x="0" y="0" width="1920" height="1080" fill="{pale}" opacity="0.58"/>',
        f'<rect x="0" y="0" width="1920" height="18" fill="{color}"/>',
        svg_text(72, 88, "Mapa de empresas por componente", 42, "#172033", 800),
        svg_text(72, 125, f"{display_gt} - parte {page}/{pages}", 27, color, 800),
        svg_text(72, 156, "Todos os componentes, numero de CNPJs unicos e duas empresas destaque por faturamento e patentes", 20, "#4a5668", 500),
    ]

    kpis = [
        ("CNPJs unicos no GT", total_unique),
        ("componentes com empresas", len(active)),
        ("componentes do GT", len(gt_rows)),
    ]
    for idx, (label, value) in enumerate(kpis):
        x = 1080 + idx * 246
        parts.append(f'<rect x="{x}" y="62" width="220" height="92" rx="8" fill="#ffffff" stroke="#d8d3c8"/>')
        parts.append(svg_text(x + 20, 102, fmt_int(value), 34, "#172033", 800))
        parts.append(svg_text(x + 20, 130, label, 16, "#5e6877", 500))

    left = 72
    top = 214
    col_w = 568
    gap = 34
    row_gap = 12
    header_h = 28
    row_h = min(142, max(120, (748 - (max_rows - 1) * row_gap) // max_rows))
    for col_idx, col_rows in enumerate(columns[:3]):
        x = left + col_idx * (col_w + gap)
        parts.append(f'<rect x="{x}" y="{top - header_h - 10}" width="{col_w}" height="{header_h}" rx="7" fill="{color}"/>')
        start_item = (page - 1) * GT_PAGE_SIZE + sum(len(prev) for prev in columns[:col_idx]) + 1
        end_item = start_item + len(col_rows) - 1
        parts.append(svg_text(x + 16, top - 18, f"Componentes {start_item}-{end_item}", 16, "#ffffff", 800))
        for idx, row in enumerate(col_rows):
            y = top + idx * (row_h + row_gap)
            count = row["cnpjs_unicos"]
            opacity = "1" if count else "0.48"
            bar_w = 0 if max_count == 0 else int((count / max_count) * 118)
            parts.append(f'<rect x="{x}" y="{y}" width="{col_w}" height="{row_h}" rx="7" fill="#ffffff" opacity="{opacity}" stroke="#d8d3c8"/>')
            parts.append(f'<rect x="{x}" y="{y}" width="6" height="{row_h}" rx="3" fill="{color}" opacity="{opacity}"/>')
            parts.append(svg_multiline(x + 18, y + 24, wrap_lines(short_component(row["componente"]), 38, 2), 16, "#1c2637", 800, 19))
            parts.append(svg_text(x + 18, y + 64, wrap_lines(row["subsistema"], 54, 1)[0], 11, "#6b7584", 500))
            parts.append(f'<rect x="{x + 398}" y="{y + 18}" width="118" height="8" rx="4" fill="{pale}"/>')
            if bar_w:
                parts.append(f'<rect x="{x + 398}" y="{y + 18}" width="{bar_w}" height="8" rx="4" fill="{color}"/>')
            parts.append(svg_text(x + 402, y + 60, metric_text(row), 32, color, 800))
            parts.append(svg_text(x + 402, y + 82, "CNPJs unicos", 11, "#687386", 700))
            parts.append(svg_text(x + 18, y + row_h - 55, "Faturamento", 11, "#687386", 800))
            for line_idx, line in enumerate(highlight_lines(row["destaques_faturamento"], 50, 2)):
                parts.append(svg_text(x + 105, y + row_h - 55 + line_idx * 14, line, 10, "#283346", 500))
            parts.append(svg_text(x + 18, y + row_h - 18, "Patentes", 11, "#687386", 800))
            for line_idx, line in enumerate(highlight_lines(row["destaques_patentes"], 56, 1)):
                parts.append(svg_text(x + 84, y + row_h - 18 + line_idx * 14, line, 10, "#283346", 500))

    parts.append(svg_text(72, 1034, "Componentes sem CNPJs aparecem esmaecidos. Fat. = 2 maiores faixas de faturamento; Pat. = 2 maiores totais de patentes.", 17, "#5e6877", 500))
    parts.append("</svg>")
    return "\n".join(parts)


def slug_gt(gt_key: str) -> str:
    return gt_key.lower().replace(" ", "_")


def write_gt_files(rows: list[dict]) -> list[tuple[str, Path, Path]]:
    outputs = []
    for old_file in OUT_DIR.glob("slide_componentes_empresas_gt_*_p*.*"):
        old_file.unlink()
    for gt_key in sorted({row["gt_key"] for row in rows if row["gt_key"].startswith("GT ")}):
        gt_rows = sorted(
            [row for row in rows if row["gt_key"] == gt_key],
            key=lambda row: (row["subsistema"], row["componente"]),
        )
        pages = max(1, (len(gt_rows) + GT_PAGE_SIZE - 1) // GT_PAGE_SIZE)
        page_svgs = []
        for page in range(1, pages + 1):
            page_rows = gt_rows[(page - 1) * GT_PAGE_SIZE : page * GT_PAGE_SIZE]
            svg = build_gt_svg(gt_key, rows, page_rows=page_rows, page=page, pages=pages)
            page_svgs.append(svg)
            stem = f"slide_componentes_empresas_{slug_gt(gt_key)}_p{page}"
            svg_path = OUT_DIR / f"{stem}.svg"
            html_path = OUT_DIR / f"{stem}.html"
            svg_path.write_text(svg, encoding="utf-8")
            html_path.write_text(
                "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Slide componentes por GT</title>"
                "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
                "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
                f"<body>{svg}</body></html>",
                encoding="utf-8",
            )
            outputs.append((f"{gt_key} p{page}", svg_path, html_path))

        first_svg = page_svgs[0]
        stem = f"slide_componentes_empresas_{slug_gt(gt_key)}"
        svg_path = OUT_DIR / f"{stem}.svg"
        html_path = OUT_DIR / f"{stem}.html"
        svg_path.write_text(first_svg, encoding="utf-8")
        html_path.write_text(
            "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Slide componentes por GT</title>"
            "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
            "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
            f"<body>{first_svg}</body></html>",
            encoding="utf-8",
        )
        outputs.append((f"{gt_key} p1 alias", svg_path, html_path))
    return outputs


def write_csv(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "gt",
                "subsistema",
                "componente",
                "cnpjs_unicos",
                "destaques_faturamento",
                "destaques_patentes",
                "component_id",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = build_data()
    write_csv(rows)
    svg = build_svg(rows)
    gt_outputs = write_gt_files(rows)
    OUT_SVG.write_text(svg, encoding="utf-8")
    OUT_HTML.write_text(
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>Slide componentes</title>"
        "<style>body{margin:0;background:#222;display:grid;place-items:center;min-height:100vh}"
        "svg{width:min(100vw,1600px);height:auto;box-shadow:0 18px 60px rgba(0,0,0,.35)}</style></head>"
        f"<body>{svg}</body></html>",
        encoding="utf-8",
    )
    print(f"OK: {OUT_SVG}")
    print(f"OK: {OUT_HTML}")
    for gt_key, svg_path, html_path in gt_outputs:
        print(f"OK {gt_key}: {svg_path}")
        print(f"OK {gt_key}: {html_path}")
    print(f"OK: {OUT_CSV}")


if __name__ == "__main__":
    main()
