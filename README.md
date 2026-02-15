# Extrator_ORCID_Crossref

App web (Streamlit) para extração e consolidação de indicadores a partir de ORCID:
**ORCID → DOI → Crossref REST (citações/metadados) + Crossref Event Data (menções por fonte) → Excel**

## Funcionalidades
- Entrada: e-mail (mailto) + upload de Excel (.xlsx) com ORCIDs
- Para cada ORCID: lista works, extrai DOI
- Para cada DOI:
  - Crossref REST: `is-referenced-by-count` e metadados
  - Crossref Event Data: contagem de eventos por fonte (source)
- Saída: Excel com 1 aba (`dados`) com:
  - Colunas bibliográficas + colunas fixas por fonte + colunas extras quando novas fontes aparecem

## Como usar (local)
```bash
pip install -r requirements.txt
streamlit run app.py
