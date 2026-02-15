import re
import time
from datetime import datetime
from io import BytesIO

import requests
import pandas as pd
import streamlit as st


# =========================
# 1) Configurações (APIs)
# =========================
ORCID_API = "https://pub.orcid.org/v3.0"
CROSSREF_WORKS_API = "https://api.crossref.org/works"
EVENTDATA_API = "https://api.eventdata.crossref.org/v1/events"

# Pausa entre requisições para evitar rate limit
SLEEP_SECONDS = 0.25

# Event Data: tamanho de página e limite de páginas (segurança)
EVENTDATA_ROWS = 1000
EVENTDATA_MAX_PAGES = 50

# Colunas FIXAS por fonte (sempre aparecem no Excel, mesmo se 0)
FONTES_FIXAS = [
    "twitter", "news", "blogs", "reddit", "wikipedia", "facebook",
    "policy", "patent", "stackexchange", "youtube", "linkedin", "unknown"
]


# =========================
# 2) Utilitários
# =========================
def get_json(url: str, headers=None, params=None, timeout=30) -> dict:
    """GET com retorno JSON e tratamento de erro HTTP."""
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def normalizar_orcid(orcid: str) -> str:
    """Normaliza ORCID para 0000-0000-0000-0000 (se vier sem hífen)."""
    o = str(orcid).strip().replace(" ", "")
    raw = o.replace("-", "")
    if len(raw) == 16:
        return f"{raw[0:4]}-{raw[4:8]}-{raw[8:12]}-{raw[12:16]}"
    return o


def extrair_doi_de_texto(texto: str):
    """Extrai DOI de texto/URL (heurística)."""
    if not texto:
        return None
    t = str(texto).strip()
    m = re.search(r"(10\.\d{4,9}/[^\s\"<>]+)", t)
    if m:
        return m.group(1).rstrip(").,;]")
    return None


def ler_orcids_do_excel(uploaded_file) -> list[str]:
    """
    Lê ORCIDs de um Excel (.xlsx).
    - Se existir coluna 'orcid' (case-insensitive), usa ela.
    - Senão, usa a primeira coluna.
    - Remove vazios, normaliza e dedup mantendo ordem.
    """
    df_orcids = pd.read_excel(uploaded_file)

    col_map = {c: str(c).strip().lower() for c in df_orcids.columns}
    col_orcid = None
    for original, low in col_map.items():
        if low == "orcid":
            col_orcid = original
            break

    if col_orcid is None:
        col_orcid = df_orcids.columns[0]

    orcids = []
    seen = set()
    for x in df_orcids[col_orcid].astype(str).tolist():
        o = x.strip()
        if not o or o.lower() in {"nan", "none"}:
            continue
        o = normalizar_orcid(o)
        if o not in seen:
            orcids.append(o)
            seen.add(o)

    return orcids


# =========================
# 3) ORCID: listar works e pegar DOI
# =========================
def listar_works_orcid(orcid: str) -> list[dict]:
    """Lista works (resumo) do ORCID."""
    headers = {"Accept": "application/json"}
    url = f"{ORCID_API}/{orcid}/works"
    data = get_json(url, headers=headers)

    works = []
    for g in data.get("group", []):
        for ws in g.get("work-summary", []):
            works.append({
                "put_code": ws.get("put-code"),
                "title": (ws.get("title") or {}).get("title", {}).get("value"),
                "type": ws.get("type"),
                "publication_year_orcid": (ws.get("publication-date") or {}).get("year", {}).get("value"),
                "source_orcid": (ws.get("source") or {}).get("source-name", {}).get("value"),
            })
    return works


def detalhes_work_orcid(orcid: str, put_code: int) -> dict:
    """Busca detalhes de um work do ORCID (onde aparecem external-ids)."""
    headers = {"Accept": "application/json"}
    url = f"{ORCID_API}/{orcid}/work/{put_code}"
    return get_json(url, headers=headers)


def extrair_doi_do_work_orcid(work_detail: dict):
    """Extrai DOI dos external-ids do work do ORCID."""
    ext_ids = (work_detail.get("external-ids") or {}).get("external-id", [])

    for eid in ext_ids:
        if (eid.get("external-id-type") or "").lower() == "doi":
            valor = (eid.get("external-id-value") or "").strip()
            return extrair_doi_de_texto(valor) or (valor or None)

    for eid in ext_ids:
        valor = (eid.get("external-id-value") or "").strip()
        doi = extrair_doi_de_texto(valor)
        if doi:
            return doi

    return None


# =========================
# 4) Crossref (bibliometria)
# =========================
def crossref_por_doi(doi: str, email: str) -> dict:
    url = f"{CROSSREF_WORKS_API}/{doi}"
    params = {"mailto": email} if email else {}
    data = get_json(url, params=params)
    msg = data.get("message", {})

    return {
        "crossref_is_referenced_by_count": msg.get("is-referenced-by-count"),
        "crossref_references_count": msg.get("references-count"),
        "crossref_container_title": (msg.get("container-title") or [None])[0],
        "crossref_publisher": msg.get("publisher"),
        "crossref_issued_year": (((msg.get("issued") or {}).get("date-parts") or [[None]])[0] or [None])[0],
    }


# =========================
# 5) Event Data (altmetria) — por fonte
# =========================
def eventdata_por_doi(doi: str, email: str, rows: int = EVENTDATA_ROWS, max_pages: int = EVENTDATA_MAX_PAGES) -> dict:
    contagem_por_fonte = {}
    cursor = None
    page = 0

    while True:
        page += 1
        if page > max_pages:
            break

        params = {"obj-id": f"https://doi.org/{doi}", "rows": rows}
        if email:
            params["mailto"] = email
        if cursor:
            params["cursor"] = cursor

        r = requests.get(EVENTDATA_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        message = (data.get("message") or {})
        events = (message.get("events") or [])

        if not events:
            break

        for ev in events:
            src = (ev.get("source") or "unknown").strip() or "unknown"
            contagem_por_fonte[src] = contagem_por_fonte.get(src, 0) + 1

        next_cursor = message.get("next-cursor")
        if not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        time.sleep(SLEEP_SECONDS)

    out = {f"eventdata_source_{s}": int(contagem_por_fonte.get(s, 0)) for s in FONTES_FIXAS}

    for fonte, cont in contagem_por_fonte.items():
        col = f"eventdata_source_{fonte}"
        if col not in out:
            out[col] = int(cont)

    return out


# =========================
# 6) Pipeline (lista ORCIDs -> DataFrame)
# =========================
def coletar_para_lista_orcids(
    orcids: list[str],
    email: str,
    logger=None,
    progress_cb=None,
) -> pd.DataFrame:
    linhas = []
    total_orcids = len(orcids)

    for idx_orcid, orcid_in in enumerate(orcids, start=1):
        orcid = normalizar_orcid(orcid_in)

        if logger:
            logger(f"[ORCID {idx_orcid}/{total_orcids}] {orcid}")

        try:
            time.sleep(SLEEP_SECONDS)
            works = listar_works_orcid(orcid)
            if logger:
                logger(f"  - Works no ORCID: {len(works)}")
        except Exception as e:
            if logger:
                logger(f"  ! Erro ao listar works do ORCID {orcid}: {e}")
            continue

        for i, w in enumerate(works, start=1):
            put_code = w["put_code"]
            title = w.get("title")

            if logger:
                logger(f"    [{i}/{len(works)}] put-code={put_code} | {title}")

            doi = None
            try:
                time.sleep(SLEEP_SECONDS)
                detail = detalhes_work_orcid(orcid, put_code)
                doi = extrair_doi_do_work_orcid(detail)
            except Exception:
                doi = None

            linha = {
                "orcid": orcid,
                "put_code": put_code,
                "title": title,
                "type": w.get("type"),
                "publication_year_orcid": w.get("publication_year_orcid"),
                "source_orcid": w.get("source_orcid"),
                "doi": doi,
            }

            if doi:
                try:
                    time.sleep(SLEEP_SECONDS)
                    linha.update(crossref_por_doi(doi, email))
                except Exception:
                    linha.update({
                        "crossref_is_referenced_by_count": None,
                        "crossref_references_count": None,
                        "crossref_container_title": None,
                        "crossref_publisher": None,
                        "crossref_issued_year": None,
                    })

                try:
                    time.sleep(SLEEP_SECONDS)
                    linha.update(eventdata_por_doi(doi, email))
                except Exception:
                    for s in FONTES_FIXAS:
                        linha[f"eventdata_source_{s}"] = 0
            else:
                for s in FONTES_FIXAS:
                    linha[f"eventdata_source_{s}"] = 0

            linhas.append(linha)

        if progress_cb:
            progress_cb(idx_orcid / max(total_orcids, 1))

    return pd.DataFrame(linhas)


def ordenar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    cols_base = [
        "orcid", "put_code", "title", "type", "publication_year_orcid", "source_orcid", "doi",
        "crossref_is_referenced_by_count", "crossref_references_count",
        "crossref_container_title", "crossref_publisher", "crossref_issued_year",
    ]

    cols_fixas = [f"eventdata_source_{s}" for s in FONTES_FIXAS if f"eventdata_source_{s}" in df.columns]

    cols_outras = sorted([
        c for c in df.columns
        if c.startswith("eventdata_source_") and c not in set(cols_fixas)
    ])

    cols_out = [c for c in cols_base if c in df.columns] + cols_fixas + cols_outras + \
               [c for c in df.columns if c not in set(cols_base + cols_fixas + cols_outras)]

    return df[cols_out].copy()


def df_para_excel_bytes(df_out: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="dados")
    return output.getvalue()


# =========================
# 7) UI (Streamlit)
# =========================
st.set_page_config(
    page_title="Extrator_ORCID_Crossref",
    page_icon="📚",
    layout="centered",
)

st.title("Extrator ORCID Crossref")
st.caption("ORCID → DOI → Crossref (citações) + Event Data (menções por fonte) → Excel")

with st.expander("Descrição metodológica", expanded=False):
    st.markdown(
        """
Aplicativo desenvolvido por Danielle Pompeu Noronha Pontes com colaborção de Célia Regina Simonetti Barbalho, Mateus Rebouças Nascimento e Raquel Santos Maciel

O **Extrator ORCID Crossref** é um aplicativo web que automatiza a coleta e o enriquecimento de informações sobre produções científicas a partir de uma **lista de ORCIDs** (enviada em Excel) e de um **e-mail institucional** (parâmetro *mailto*). A aplicação consulta a **ORCID Public API** para recuperar os *works* dos pesquisadores e identificar **DOIs**; em seguida, utiliza a **Crossref REST API** para obter metadados e indicadores bibliométricos (como contagem de citações na cobertura Crossref) e a **Crossref Event Data** para contabilizar **menções online por fonte** (altmetria). Ao final, o sistema consolida os resultados em um **arquivo Excel (.xlsx)** com uma aba única (“dados”), pronto para download e posterior análise bibliométrica e altmétrica.

Em resumo este aplicativo implementa um pipeline de coleta baseado em APIs públicas:
- **ORCID**: lista *works* por pesquisador e identifica DOIs (quando disponíveis);
- **Crossref REST**: recupera metadados e contagem de citações (*is-referenced-by-count*);
- **Crossref Event Data**: agrega menções por **fonte** (*source*), com colunas fixas e colunas extras dinâmicas.

A saída é um arquivo **Excel (.xlsx)** contendo **uma aba** chamada **“dados”**.
        """.strip()
    )

st.subheader("Entradas")

email = st.text_input(
    "Email institucional (para o parâmetro mailto nas APIs da Crossref)",
    placeholder="nome@instituicao.br",
)

uploaded = st.file_uploader(
    "Upload do Excel (.xlsx) com a lista de ORCIDs",
    type=["xlsx"],
    accept_multiple_files=False
)

with st.expander("Formato do Excel esperado", expanded=False):
    st.markdown(
        """
- Recomendado: uma coluna chamada **orcid**  
- Alternativa: se não existir **orcid**, a primeira coluna da planilha será utilizada
        """.strip()
    )

run = st.button("Executar extração", type="primary", use_container_width=True)

st.divider()

if run:
    if not email.strip():
        st.error("Informe um email para uso no parâmetro **mailto**.")
        st.stop()
    if uploaded is None:
        st.error("Envie um arquivo Excel (.xlsx) com a lista de ORCIDs.")
        st.stop()

    try:
        orcids = ler_orcids_do_excel(uploaded)
    except Exception as e:
        st.error(f"Não foi possível ler o Excel enviado. Detalhes: {e}")
        st.stop()

    if not orcids:
        st.warning("Nenhum ORCID válido foi encontrado no Excel.")
        st.stop()

    st.success(f"ORCIDs carregados: {len(orcids)}")
    progress = st.progress(0.0)
    log_box = st.empty()

    logs = []

    def logger(msg: str):
        logs.append(msg)
        log_box.code("\n".join(logs[-40:]), language="text")

    def progress_cb(p: float):
        progress.progress(min(max(p, 0.0), 1.0))

    with st.spinner("Coletando dados (ORCID → Crossref → Event Data)..."):
        df = coletar_para_lista_orcids(
            orcids=orcids,
            email=email.strip(),
            logger=logger,
            progress_cb=progress_cb,
        )

    if df.empty:
        st.warning("A coleta foi concluída, mas não gerou linhas (verifique ORCIDs e disponibilidade nas APIs).")
        st.stop()

    df_out = ordenar_colunas(df)
    excel_bytes = df_para_excel_bytes(df_out)

    filename = f"orcid_crossref_eventdata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    st.success(f"Coleta finalizada. Linhas: {len(df_out)}")
    st.dataframe(df_out.head(25), use_container_width=True)

    st.download_button(
        label="📥 Baixar Excel (aba: dados)",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    with st.expander("Log completo", expanded=False):
        st.code("\n".join(logs), language="text")

st.markdown("---")

with st.expander("📚 Como citar este software"):
    st.markdown("""
    **PONTES, D.; NASCIMENTO, M. R.; MACIEL, R. S.; BARBALHO, C. S.**  
    ORCID-Extractor Acadêmico: ferramenta para enriquecimento automatizado de dados científicos via ORCID. Zenodo, 2026.  
    DOI: https://doi.org/10.5281/zenodo.18652894
    """)

    st.markdown("#### 📌 BibTeX")
    st.code("""
@software{pontes2026orcid,
  author       = {Pontes, Danielle and Nascimento, Mateus Rebouças and Maciel, Raquel Santos and Barbalho, Célia Simonetti},
  title        = {ORCID-Extractor Acadêmico},
  year         = {2026},
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.18652894},
  url          = {https://doi.org/10.5281/zenodo.18652894}
}
""", language="bibtex")




