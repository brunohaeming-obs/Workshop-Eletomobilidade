from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import NA_VALUE
from loaders import component_cnae_map, load_empresas


ROOT = Path(__file__).resolve().parent
OUT_CSV = ROOT / "dados_resumo" / "empresas_por_subsistema.csv"
OUT_HTML = ROOT / "slide_empresas_por_subsistema.html"


def esc(value: object) -> str:
    return html.escape(str(value))


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}".replace(",", ".")


def build_data() -> pd.DataFrame:
    empresas = load_empresas()
    mapped = component_cnae_map()
    rows = []
    for (gt, subsistema), part in mapped.groupby(["gt", "subsistema"], sort=False):
        groups = {str(value) for value in part["cnae_grupo"] if str(value) != NA_VALUE}
        companies = empresas[empresas["cnae_grupo"].astype(str).isin(groups)] if groups else empresas.iloc[0:0]
        rows.append(
            {
                "GT": gt,
                "Subsistema": subsistema,
                "CNPJs unicos": companies["nr_cnpj"].astype(str).nunique(),
                "Municipios": companies["cd_municipio_ibge"].nunique(),
                "Componentes": part["component_id"].nunique(),
                "NCMs destacados": part["ncm"].astype(str).nunique(),
            }
        )
    return pd.DataFrame(rows).sort_values("CNPJs unicos", ascending=False)


def build_slide(df: pd.DataFrame) -> str:
    top = df.head(12).copy()
    max_value = max(int(top["CNPJs unicos"].max()), 1)
    total_subsystems = df["Subsistema"].nunique()
    total_cnpjs_sum = int(df["CNPJs unicos"].sum())
    rows = []
    for item in top.to_dict(orient="records"):
        width = int(item["CNPJs unicos"]) / max_value * 100
        rows.append(
            f"""
            <div class="bar">
              <div class="label">
                <strong>{esc(item['Subsistema'])}</strong>
                <span>{esc(item['GT'])}</span>
              </div>
              <div class="track"><i style="width:{width:.2f}%"></i></div>
              <div class="value">{fmt_int(item['CNPJs unicos'])}</div>
            </div>
            """
        )
    table_rows = "".join(
        f"<tr><td>{esc(row['Subsistema'])}</td><td>{esc(row['GT'])}</td><td>{fmt_int(row['Componentes'])}</td><td>{fmt_int(row['NCMs destacados'])}</td><td>{fmt_int(row['Municipios'])}</td><td><strong>{fmt_int(row['CNPJs unicos'])}</strong></td></tr>"
        for row in top.head(8).to_dict(orient="records")
    )
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Empresas por subsistema</title>
  <style>
    @page {{ size: 16in 9in; margin: 0; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; background:#0b1422; color:#edf6ff; font-family:Inter,Segoe UI,Arial,sans-serif; }}
    .slide {{ width:1600px; height:900px; padding:58px 70px; background:linear-gradient(135deg,#0b1422 0%,#10243b 62%,#0e322c 100%); }}
    header {{ display:flex; justify-content:space-between; align-items:flex-start; gap:40px; margin-bottom:34px; }}
    h1 {{ margin:0; font-size:54px; line-height:1.02; letter-spacing:0; max-width:970px; }}
    .subtitle {{ margin-top:14px; color:#b8cee3; font-size:23px; max-width:980px; }}
    .kpis {{ display:grid; grid-template-columns:1fr; gap:12px; min-width:260px; }}
    .kpi {{ border:1px solid rgba(184,206,227,.24); border-radius:8px; padding:15px 18px; background:rgba(255,255,255,.06); }}
    .kpi strong {{ display:block; color:#72e3ff; font-size:36px; line-height:1; }}
    .kpi span {{ display:block; color:#b8cee3; font-size:15px; margin-top:6px; }}
    main {{ display:grid; grid-template-columns:1.08fr .92fr; gap:34px; align-items:start; }}
    .panel {{ border:1px solid rgba(184,206,227,.22); border-radius:10px; background:rgba(5,16,29,.58); padding:24px; }}
    h2 {{ margin:0 0 18px; font-size:25px; color:#dff6ff; }}
    .bar {{ display:grid; grid-template-columns:285px minmax(0,1fr) 90px; gap:14px; align-items:center; padding:10px 0; border-bottom:1px solid rgba(184,206,227,.14); }}
    .bar:last-child {{ border-bottom:0; }}
    .label strong {{ display:block; font-size:17px; color:#fff; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .label span {{ display:block; color:#93abc2; font-size:13px; margin-top:2px; }}
    .track {{ height:17px; background:rgba(255,255,255,.09); border-radius:999px; overflow:hidden; }}
    .track i {{ display:block; height:100%; background:linear-gradient(90deg,#55c7ff,#8df0b2); border-radius:999px; }}
    .value {{ text-align:right; color:#82e8ff; font-size:19px; font-weight:800; }}
    table {{ width:100%; border-collapse:collapse; font-size:17px; }}
    th,td {{ padding:11px 10px; border-bottom:1px solid rgba(184,206,227,.14); text-align:left; vertical-align:top; }}
    th {{ color:#9fc1df; font-size:13px; text-transform:uppercase; }}
    td:nth-child(n+3), th:nth-child(n+3) {{ text-align:right; }}
    td:first-child {{ color:#fff; font-weight:700; }}
    footer {{ margin-top:22px; color:#9fb7cc; font-size:15px; }}
  </style>
</head>
<body>
  <section class="slide">
    <header>
      <div>
        <h1>Empresas relacionadas à cadeia por subsistema</h1>
        <p class="subtitle">Ranking dos subsistemas com maior densidade de CNPJs ativos associados por CNAE aos NCMs destacados.</p>
      </div>
      <div class="kpis">
        <div class="kpi"><strong>{fmt_int(total_subsystems)}</strong><span>subsistemas mapeados</span></div>
        <div class="kpi"><strong>{fmt_int(total_cnpjs_sum)}</strong><span>soma de CNPJs por subsistema</span></div>
      </div>
    </header>
    <main>
      <section class="panel">
        <h2>Top 12 subsistemas por CNPJs únicos</h2>
        {''.join(rows)}
      </section>
      <section class="panel">
        <h2>Detalhe para leitura executiva</h2>
        <table>
          <thead><tr><th>Subsistema</th><th>GT</th><th>Comp.</th><th>NCMs</th><th>Mun.</th><th>CNPJs</th></tr></thead>
          <tbody>{table_rows}</tbody>
        </table>
        <footer>Nota: um mesmo CNPJ pode aparecer em mais de um subsistema quando seu CNAE se relaciona a múltiplos NCMs/componentes.</footer>
      </section>
    </main>
  </section>
</body>
</html>"""


def main() -> None:
    ROOT.joinpath("dados_resumo").mkdir(parents=True, exist_ok=True)
    df = build_data()
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_HTML.write_text(build_slide(df), encoding="utf-8")
    print(f"OK: {OUT_HTML}")
    print(f"OK: {OUT_CSV}")


if __name__ == "__main__":
    main()
