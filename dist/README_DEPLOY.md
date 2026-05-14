# Deploy

Esta pasta e autocontida para deploy estatico.

## Teste local

```powershell
cd dist
python -m http.server 8000
```

Acesse:

```text
http://localhost:8000/
```

## Vercel

```powershell
vercel dist
vercel dist --prod
```

Arquivos principais:

- `index.html`: visualizacao principal Empresas com filtro NCM integrado.
- `ncm.html`: visualizacao auxiliar NCM x Empresas.
- `componentes.html`: visualizacao da cadeia de componentes e maturidade por patentes.
- `assets/data/*.json`: payloads agregados carregados pela aplicacao.
- `empresas_app_data/municipios/*.json`: detalhes por municipio carregados sob demanda.
- `empresas_app_data/componentes/*.json`: CNPJs relacionados a cada componente.
