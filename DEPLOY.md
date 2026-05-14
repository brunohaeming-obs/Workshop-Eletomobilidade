# Deploy da ferramenta

O pacote pronto para publicacao fica na pasta `dist`.

## Teste local

```powershell
cd "C:\Users\ailton-junior\1-projetos\Workshop-Eletomobilidade\dist"
python -m http.server 8000
```

Acesse:

```text
http://localhost:8000/
```

## Deploy no Vercel

Execute o deploy apontando diretamente para `dist`:

```powershell
cd "C:\Users\ailton-junior\1-projetos\Workshop-Eletomobilidade"
vercel dist
```

Para producao:

```powershell
vercel dist --prod
```

## Arquivos publicados

- `dist/index.html`: visualizacao principal Empresas, com filtro NCM integrado.
- `dist/ncm.html`: visualizacao auxiliar NCM x Empresas.
- `dist/componentes.html`: visualizacao da cadeia de componentes e indicadores INPI.
- `dist/assets/data/*.json`: dados agregados carregados pela aplicacao.
- `dist/empresas_app_data/municipios/*.json`: detalhes por municipio carregados sob demanda.
- `dist/empresas_app_data/componentes/*.json`: empresas por componente carregadas sob demanda.

Nao faca deploy da raiz do projeto, pois ela contem as planilhas originais em `dados/`.
