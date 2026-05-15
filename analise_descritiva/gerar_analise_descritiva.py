from __future__ import annotations

import html
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import NA_VALUE
from enrichers import enrich_empresas_with_patents
from loaders import component_cnae_map, load_empresas


ROOT = Path(__file__).resolve().parent
OUT_DATA = ROOT / "dados_resumo"
REPORT = ROOT / "relatorio_descritivo.html"


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}".replace(",", ".")


def fmt_pct(value: float) -> str:
    return f"{value:.1f}%".replace(".", ",")


def esc(value: object) -> str:
    return html.escape(str(value))


def write_table(df: pd.DataFrame, name: str) -> str:
    OUT_DATA.mkdir(parents=True, exist_ok=True)
    path = OUT_DATA / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path.name


def html_table(df: pd.DataFrame, max_rows: int = 15) -> str:
    if df.empty:
        return "<p class='muted'>Sem registros para este recorte.</p>"
    head = "".join(f"<th>{esc(col)}</th>" for col in df.columns)
    rows = []
    for row in df.head(max_rows).itertuples(index=False):
        cells = "".join(f"<td>{esc(value)}</td>" for value in row)
        rows.append(f"<tr>{cells}</tr>")
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"


def bar_table(df: pd.DataFrame, label: str, value: str, max_rows: int = 12) -> str:
    if df.empty:
        return "<p class='muted'>Sem registros para este recorte.</p>"
    max_value = max(float(df[value].max()), 1)
    rows = []
    for row in df.head(max_rows).to_dict(orient="records"):
        width = max(4, float(row[value]) / max_value * 100)
        rows.append(
            "<div class='bar-row'>"
            f"<span>{esc(row[label])}</span>"
            f"<strong>{fmt_int(row[value])}</strong>"
            f"<i style='width:{width:.2f}%'></i>"
            "</div>"
        )
    return "".join(rows)


def metric_card(title: str, value: str, note: str) -> str:
    return f"<div class='metric'><strong>{esc(value)}</strong><span>{esc(title)}</span><small>{esc(note)}</small></div>"


def summarize() -> dict[str, pd.DataFrame | dict]:
    print("Carregando empresas ativas...")
    empresas = load_empresas()
    print("Enriquecendo com patentes...")
    empresas = enrich_empresas_with_patents(empresas)
    print("Carregando cadeia GT/NCM...")
    mapped = component_cnae_map()

    active_cnpjs = set(empresas["nr_cnpj"].astype(str))
    chain_groups = {str(value) for value in mapped["cnae_grupo"] if str(value) != NA_VALUE}
    empresas_cadeia = empresas[empresas["cnae_grupo"].astype(str).isin(chain_groups)].copy()
    chain_cnpjs = set(empresas_cadeia["nr_cnpj"].astype(str))

    component_rows = []
    component_ncm_rows = []
    component_city_rows = []
    for component_id, part in mapped.groupby("component_id", sort=False):
        first = part.iloc[0]
        groups = {str(value) for value in part["cnae_grupo"] if str(value) != NA_VALUE}
        ncms = sorted({str(value) for value in part["ncm"] if str(value) != NA_VALUE})
        companies = empresas[empresas["cnae_grupo"].astype(str).isin(groups)].copy() if groups else empresas.iloc[0:0].copy()
        company_cnpjs = companies["nr_cnpj"].astype(str).nunique()
        patent_ids = sorted({pid for ids in companies["patentes_ids"] for pid in (ids if isinstance(ids, list) else [])})
        component_rows.append(
            {
                "GT": first["gt"],
                "Subsistema": first["subsistema"],
                "Componente": first["componente"],
                "Criticidade": first.get("criticidade", NA_VALUE),
                "Complexidade": first.get("complexidade", NA_VALUE),
                "NCMs destacados": len(ncms),
                "Grupos CNAE ligados": len(groups),
                "CNPJs unicos": company_cnpjs,
                "Municipios": companies["cd_municipio_ibge"].nunique() if not companies.empty else 0,
                "CNPJs com patente": int(companies["tem_patente"].sum()) if not companies.empty else 0,
                "Patentes distintas": len(patent_ids),
            }
        )
        for ncm, ncm_part in part.groupby("ncm", sort=False):
            ncm_groups = {str(value) for value in ncm_part["cnae_grupo"] if str(value) != NA_VALUE}
            ncm_companies = empresas[empresas["cnae_grupo"].astype(str).isin(ncm_groups)] if ncm_groups else empresas.iloc[0:0]
            component_ncm_rows.append(
                {
                    "GT": first["gt"],
                    "Subsistema": first["subsistema"],
                    "Componente": first["componente"],
                    "NCM": str(ncm),
                    "Descricao NCM": str(ncm_part["ncm_descricao_gt"].dropna().iloc[0]) if not ncm_part.empty else "",
                    "CNPJs unicos": ncm_companies["nr_cnpj"].astype(str).nunique(),
                    "Municipios": ncm_companies["cd_municipio_ibge"].nunique(),
                }
            )
        if not companies.empty:
            city = (
                companies.groupby(["sg_uf", "nm_municipio"], dropna=False)
                .agg(CNPJs=("nr_cnpj", "nunique"), Patentes=("patentes_total", "sum"))
                .reset_index()
            )
            city["GT"] = first["gt"]
            city["Componente"] = first["componente"]
            component_city_rows.append(city)

    componentes = pd.DataFrame(component_rows)
    ncm_componentes = pd.DataFrame(component_ncm_rows)
    cidades_componentes = pd.concat(component_city_rows, ignore_index=True) if component_city_rows else pd.DataFrame()

    by_uf_total = (
        empresas.groupby("sg_uf", dropna=False)
        .agg(CNPJs=("nr_cnpj", "nunique"), Municipios=("cd_municipio_ibge", "nunique"))
        .reset_index()
        .rename(columns={"sg_uf": "UF"})
        .sort_values("CNPJs", ascending=False)
    )
    by_uf_chain = (
        empresas_cadeia.groupby("sg_uf", dropna=False)
        .agg(CNPJs=("nr_cnpj", "nunique"), Municipios=("cd_municipio_ibge", "nunique"))
        .reset_index()
        .rename(columns={"sg_uf": "UF"})
        .sort_values("CNPJs", ascending=False)
    )
    by_revenue_chain = (
        empresas_cadeia.groupby("ds_faixa_faturamento_grupo", dropna=False)["nr_cnpj"]
        .nunique()
        .reset_index(name="CNPJs")
        .rename(columns={"ds_faixa_faturamento_grupo": "Faixa de faturamento"})
        .sort_values("CNPJs", ascending=False)
    )
    by_cnae_chain = (
        empresas_cadeia.groupby(["cnae_grupo", "cnae_grupo_nome"], dropna=False)["nr_cnpj"]
        .nunique()
        .reset_index(name="CNPJs")
        .rename(columns={"cnae_grupo": "CNAE grupo", "cnae_grupo_nome": "Descricao"})
        .sort_values("CNPJs", ascending=False)
    )
    by_city_chain = (
        empresas_cadeia.groupby(["sg_uf", "nm_municipio"], dropna=False)
        .agg(CNPJs=("nr_cnpj", "nunique"), Patentes=("patentes_total", "sum"))
        .reset_index()
        .rename(columns={"sg_uf": "UF", "nm_municipio": "Municipio"})
        .sort_values(["CNPJs", "Patentes"], ascending=False)
    )
    by_gt = (
        componentes.groupby("GT", dropna=False)
        .agg(
            Componentes=("Componente", "nunique"),
            NCMs=("NCMs destacados", "sum"),
            CNPJs=("CNPJs unicos", "sum"),
            Municipios=("Municipios", "max"),
            Patentes=("Patentes distintas", "sum"),
        )
        .reset_index()
        .sort_values("CNPJs", ascending=False)
    )
    risk_matrix = (
        componentes.groupby(["Complexidade", "Criticidade"], dropna=False)
        .agg(Componentes=("Componente", "nunique"), CNPJs=("CNPJs unicos", "sum"), Patentes=("Patentes distintas", "sum"))
        .reset_index()
        .sort_values(["Criticidade", "Complexidade"], ascending=False)
    )
    gaps = ncm_componentes[ncm_componentes["CNPJs unicos"].eq(0)].sort_values(["GT", "Componente", "NCM"])

    patent_ids_chain = sorted({pid for ids in empresas_cadeia["patentes_ids"] for pid in (ids if isinstance(ids, list) else [])})
    metrics = {
        "cnpjs_ativos": len(active_cnpjs),
        "cnpjs_cadeia": len(chain_cnpjs),
        "aderencia_pct": len(chain_cnpjs) / max(len(active_cnpjs), 1) * 100,
        "municipios_ativos": int(empresas["cd_municipio_ibge"].nunique()),
        "municipios_cadeia": int(empresas_cadeia["cd_municipio_ibge"].nunique()),
        "componentes": int(componentes["Componente"].nunique()),
        "ncms_destacados": int(mapped["ncm"].astype(str).nunique()),
        "ncms_com_cnpj": int(ncm_componentes["NCM"].loc[ncm_componentes["CNPJs unicos"].gt(0)].nunique()),
        "patentes_distintas_cadeia": len(patent_ids_chain),
        "cnpjs_com_patente_cadeia": int(empresas_cadeia["tem_patente"].sum()),
    }

    return {
        "metrics": metrics,
        "by_uf_total": by_uf_total,
        "by_uf_chain": by_uf_chain,
        "by_revenue_chain": by_revenue_chain,
        "by_cnae_chain": by_cnae_chain,
        "by_city_chain": by_city_chain,
        "by_gt": by_gt,
        "componentes": componentes.sort_values("CNPJs unicos", ascending=False),
        "ncm_componentes": ncm_componentes.sort_values("CNPJs unicos", ascending=False),
        "risk_matrix": risk_matrix,
        "gaps": gaps,
        "cidades_componentes": cidades_componentes.sort_values("CNPJs", ascending=False) if not cidades_componentes.empty else cidades_componentes,
    }


def build_report(results: dict[str, pd.DataFrame | dict]) -> str:
    metrics = results["metrics"]
    assert isinstance(metrics, dict)
    tables = {name: value for name, value in results.items() if isinstance(value, pd.DataFrame)}
    written = {name: write_table(df, name) for name, df in tables.items()}

    cards = [
        metric_card("CNPJs ativos na base", fmt_int(metrics["cnpjs_ativos"]), "Universo filtrado pela base ativa Databricks."),
        metric_card("CNPJs aderentes a cadeia", fmt_int(metrics["cnpjs_cadeia"]), f"{fmt_pct(metrics['aderencia_pct'])} do universo ativo."),
        metric_card("Municipios com empresas aderentes", fmt_int(metrics["municipios_cadeia"]), f"de {fmt_int(metrics['municipios_ativos'])} municipios ativos."),
        metric_card("Componentes mapeados", fmt_int(metrics["componentes"]), "Hierarquia GT > subsistema > componente."),
        metric_card("NCMs destacados", fmt_int(metrics["ncms_destacados"]), f"{fmt_int(metrics['ncms_com_cnpj'])} com CNPJs ligados por CNAE."),
        metric_card("Patentes distintas na cadeia", fmt_int(metrics["patentes_distintas_cadeia"]), f"{fmt_int(metrics['cnpjs_com_patente_cadeia'])} CNPJs com patente."),
    ]

    by_gt = results["by_gt"]
    by_city = results["by_city_chain"]
    by_revenue = results["by_revenue_chain"]
    by_cnae = results["by_cnae_chain"]
    risk = results["risk_matrix"]
    gaps = results["gaps"]
    componentes = results["componentes"]

    assert isinstance(by_gt, pd.DataFrame)
    assert isinstance(by_city, pd.DataFrame)
    assert isinstance(by_revenue, pd.DataFrame)
    assert isinstance(by_cnae, pd.DataFrame)
    assert isinstance(risk, pd.DataFrame)
    assert isinstance(gaps, pd.DataFrame)
    assert isinstance(componentes, pd.DataFrame)

    metadata = {
        "arquivos_csv": written,
        "observacao": "Cadeia produtiva inferida por CNAE associado ao NCM; nao representa transacao ou capacidade instalada fisica.",
    }
    (ROOT / "manifest.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Análise descritiva da cadeia de eletromobilidade</title>
  <style>
    :root {{ --bg:#f6f8fb; --ink:#172033; --muted:#5f6f86; --line:#d9e2ee; --blue:#1261a6; --green:#18865b; --amber:#a96500; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:var(--bg); color:var(--ink); line-height:1.45; }}
    header {{ padding:36px 44px 24px; background:#0d2238; color:white; }}
    header p {{ max-width:980px; color:#c8d7e8; }}
    main {{ padding:28px 44px 48px; max-width:1240px; margin:auto; }}
    h1 {{ margin:0 0 8px; font-size:30px; }}
    h2 {{ margin:32px 0 12px; font-size:20px; color:#0d2238; }}
    h3 {{ margin:22px 0 10px; font-size:16px; }}
    .metrics {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:14px; margin:22px 0; }}
    .metric {{ background:white; border:1px solid var(--line); border-radius:8px; padding:16px; }}
    .metric strong {{ display:block; font-size:26px; color:var(--blue); }}
    .metric span {{ display:block; font-weight:700; margin-top:3px; }}
    .metric small,.muted {{ color:var(--muted); }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .panel {{ background:white; border:1px solid var(--line); border-radius:8px; padding:16px; }}
    .bar-row {{ position:relative; display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; padding:9px 0; border-bottom:1px solid #edf1f6; overflow:hidden; }}
    .bar-row span,.bar-row strong {{ position:relative; z-index:1; }}
    .bar-row i {{ position:absolute; left:0; bottom:0; height:3px; background:linear-gradient(90deg,var(--blue),#77b7e7); }}
    .table-wrap {{ overflow:auto; border:1px solid var(--line); border-radius:8px; background:white; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ padding:9px 10px; border-bottom:1px solid #edf1f6; text-align:left; vertical-align:top; }}
    th {{ background:#eef4fb; color:#23415f; position:sticky; top:0; }}
    .note {{ border-left:4px solid var(--amber); background:#fff8ed; padding:12px 14px; margin:16px 0; color:#5d4630; }}
    a {{ color:var(--blue); }}
    @media (max-width:900px) {{ header,main {{ padding-left:20px; padding-right:20px; }} .metrics,.grid {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <header>
    <h1>Análise descritiva da capacidade produtiva inferida</h1>
    <p>Leitura executiva da base de empresas ativas, do vínculo NCM-CNAE-CNPJ e da hierarquia GT > subsistema > componente. O foco é apoiar relatório sobre concentração territorial, cobertura da cadeia e sinais de maturidade tecnológica.</p>
  </header>
  <main>
    <section class="metrics">{''.join(cards)}</section>
    <div class="note"><strong>Critério:</strong> empresa aderente à cadeia é o CNPJ ativo cujo grupo CNAE está associado a ao menos um NCM destacado na base GT/NCM. A análise mede presença empresarial e densidade tecnológica inferida; não mede capacidade instalada, volume produzido ou transações reais.</div>

    <section class="grid">
      <div class="panel"><h2>Distribuição por GT</h2>{bar_table(by_gt, "GT", "CNPJs")}</div>
      <div class="panel"><h2>Principais municípios da cadeia</h2>{bar_table(by_city.assign(Local=by_city["Municipio"] + "/" + by_city["UF"]), "Local", "CNPJs")}</div>
      <div class="panel"><h2>Faixa de faturamento</h2>{bar_table(by_revenue, "Faixa de faturamento", "CNPJs")}</div>
      <div class="panel"><h2>Principais grupos CNAE</h2>{bar_table(by_cnae.assign(CNAE=by_cnae["CNAE grupo"] + " - " + by_cnae["Descricao"]), "CNAE", "CNPJs")}</div>
    </section>

    <h2>Complexidade x criticidade</h2>
    <p class="muted">A matriz combina a nova classificação do GT/NCM com a quantidade de componentes, CNPJs relacionados e patentes distintas.</p>
    {html_table(risk, 20)}

    <h2>Componentes com maior presença empresarial</h2>
    {html_table(componentes[["GT", "Subsistema", "Componente", "Criticidade", "Complexidade", "NCMs destacados", "CNPJs unicos", "Municipios", "Patentes distintas"]], 25)}

    <h2>Lacunas de cobertura NCM</h2>
    <p class="muted">NCMs destacados que não encontraram CNPJs ativos por meio do grupo CNAE vinculado.</p>
    {html_table(gaps[["GT", "Subsistema", "Componente", "NCM", "Descricao NCM"]], 30)}

    <h2>Arquivos gerados</h2>
    <p>As tabelas completas foram exportadas para <code>analise_descritiva/dados_resumo/</code>:</p>
    <ul>{''.join(f"<li><a href='dados_resumo/{esc(path)}'>{esc(path)}</a></li>" for path in written.values())}</ul>
  </main>
</body>
</html>"""


def main() -> None:
    OUT_DATA.mkdir(parents=True, exist_ok=True)
    results = summarize()
    html_text = build_report(results)
    REPORT.write_text(html_text, encoding="utf-8")
    print(f"OK: {REPORT}")
    print(f"OK: {OUT_DATA}")


if __name__ == "__main__":
    main()
