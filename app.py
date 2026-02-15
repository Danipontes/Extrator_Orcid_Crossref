import re
import time
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
import streamlit as st

# =========================
# Identidade do App
# =========================
APP_NAME = "Extrator_ORCID_Crossref"
APP_SUBTITLE = "ORCID → DOI → Crossref (citações/metadados) + Event Data (menções por fonte) → Excel"

# =========================
# Configurações de APIs
# =========================
ORCID_API = "https://pub.orcid.org/v3.0"
CROSSREF_WORKS_API = "https://api.crossref.org/works"
EVENTDATA_API = "https://api.eventdata.crossref.org/v1/events"

DEFAULT_SLEEP_SECONDS = 0.25
DEFAULT_EVENTDATA_ROWS = 1000
DEFAULT_EVENTDATA_MAX_PAGES = 50

DEFAULT_FONTES_FIXAS = [
    "twitter", "news", "blogs", "reddit", "wikipedia", "facebook",
    "policy", "patent", "stackexchange", "youtube", "linkedin", "unknown"
]

ORCID_REGEX = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", re.IGNORECASE)

# =========================
# Estilo (acadêmico/científico)
# =========================
def inject_css():
    st.markdown(
        """
        <style>
          /* Layout geral */
          .block-container { padding-top: 2.2rem; padding-bottom: 2.2rem; max-width: 1100px; }
          /* Tipografia */
          html, body, [class*="css"]  { font-family: "Inter", "Segoe UI", system-ui, -apple-system, Arial, sans-serif; }
          h1, h2, h3 { letter-spacing: -0.02em; }
          /* Cabeçalho */
          .hero {
            border: 1px solid rgba(60,60,60,0.15);
            border-radius: 18px;
            padding: 18px 18px 14px 18px;
            background: linear-gradient(180deg, rgba(250,250,250,0.92), rgba(250,250,250,0.65));
            box-shadow: 0 6px 22px rgba(0,0,0,0.06);
          }
          .hero-title { font-size: 1.6rem; font-weight: 700; margin: 0 0 0.2rem 0; }
          .hero-subtitle { font-size: 1.02rem; opacity: 0.85; margin: 0.1rem 0 0.2rem 0; }
          .hero-caption { font-size: 0.92rem; opacity: 0.75; margin: 0.35rem 0 0 0; }
          /* Cards */
          .card {
            border: 1px solid rgba(60,60,60,0.12);
            border-radius: 16px;
            padding: 14px 14px 12px 14px;
            background: rgba(255,255,255,0.72);
            box-shadow: 0 4px 16px rgba(0,0,0,0.04);
          }
          /* Badges */
          .badge {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            border: 1px solid rgba(60,60,60,0.16);
            font-size: 0.82rem;
            opacity: 0.85;
            margin-right: 0.35rem;
          }
          /* Tabelas */
          .stDataFrame { border-radius: 12px; overflow: hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# =========================
# Utilidades de rede e parsing
# =========================
def get_json(url: str, headers=None, params=None, timeout=30) -> dict:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def normalizar_orcid(orcid: str) -> str:
    o = str(orcid).strip().replace(" ", "")
    raw = o.replace("-", "")
    if len(raw) == 16:
        return f"{raw[0:4]}-{raw[4:8]}-{raw[8:12]}-{raw[12:16]}"
    return o

def validar_orcid(orcid: str) -> bool:
    o = normalizar_orcid(orcid)
    return bool(ORCID_REGEX.match(o))

def extrair_doi_de_texto(texto: str):
    if not texto:
        return None
    t = str(texto).strip()
    m = re.search(r"(10\.\d{4,9}/[^\s\"<>]+)", t)
    if m:
        return m.group(1).rstrip(").,;]")
    return None

# =========================
# ORCID
# =========================
def listar_works_orcid(orcid: str) -> list[dict]:
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
    headers = {"Accept": "application/json"}
    url = f"{ORCID_API}/{orcid}/work/{put_code}"
    return get_json(url, headers=headers)

def extrair_doi_do_work_orcid(work_detail: dict):
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
# Crossref (bibliometria / metadados)
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
# Event Data (menções por fonte)
# =========================
def eventdata_por_doi(
    doi: str,
    email: str,
    fontes_fixas: list[str],
    sleep_seconds: float,
    rows: int = 1000,
    max_pages: int = 50
) -> dict:
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
            src = (ev.get("source") or "unknown").strip()
            contagem_por_fonte[src] = contagem_por_fonte.get(src, 0) + 1

        next_cursor = message.get("next-cursor")
        if not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        time.sleep(sleep_seconds)

    out = {f"eventdata_source_{s}": int(contagem_por_fonte.get(s, 0)) for s in fontes_fixas}

    for fonte, cont in contagem_por_fonte.items():
        col = f"eventdata_source_{fonte}"
        if col not in out:
            out[col] = int(cont)

    return out

# =========================
# Pipeline
# =========================
def coletar_para_lista_orcids(
    orcids: list[str],
    email: str,
    fontes_fixas: list[str],
    sleep_seconds: float,
    eventdata_rows: int,
    eventdata_max_pages: int,
    progress_cb=None,
    log_cb=None
) -> pd.DataFrame:
    linhas = []
    total_orcids = len(orcids)

    for idx_orcid, orcid_in in enumerate(orcids, start=1):
        orcid = normalizar_orcid(orcid_in)

        if log_cb:
            log_cb(f"[ORCID {idx_orcid}/{total_orcids}] {orcid}")

        try:
            time.sleep(sleep_seconds)
            works = listar_works_orcid(orcid)
            if log_cb:
                log_cb(f"  - Works no ORCID: {len(works)}")
        except Exception as e:
            if log_cb:
                log_cb(f"  ! Erro ao listar works do ORCID {orcid}: {e}")
            continue

        # Atualiza progresso por ORCID (macro)
        if progress_cb:
            progress_cb(idx_orcid / max(total_orcids, 1))

        for i, w in enumerate(works, start=1):
            put_code = w["put_code"]
            title = w.get("title")

            if log_cb:
                log_cb(f"    [{i}/{len(works)}] put-code={put_code} | {title}")

            doi = None
            try:
                time.sleep(sleep_seconds)
                detail = detalhes_work_orcid(orcid, put_code)
                doi = extrair_doi_do_work_orcid(detail)
            except Exception:
                pass

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
                    time.sleep(sleep_seconds)
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
                    time.sleep(sleep_seconds)
                    linha.update(
                        eventdata_por_doi(
                            doi=doi,
                            email=email,
                            fontes_fixas=fontes_fixas,
                            sleep_seconds=sleep_seconds,
                            rows=eventdata_rows,
                            max_pages=eventdata_max_pages,
                        )
                    )
                except Exception:
                    for s in fontes_fixas:
                        linha[f"eventdata_source_{s}"] = 0
            else:
                for s in fontes_fixas:
                    linha[f"eventdata_source_{s}"] = 0

            linhas.append(linha)

    return pd.DataFrame(linhas)

def ordenar_colunas(df: pd.DataFrame, fontes_fixas: list[str]) -> pd.DataFrame:
    cols_base = [
        "orcid", "put_code", "title", "type", "publication_year_orcid", "source_orcid", "doi",
        "crossref_is_referenced_by_count", "crossref_references_count",
        "crossref_container_title", "crossref_publisher", "crossref_issued_year",
    ]
    cols_fixas = [f"eventdata_source_{s}" for s in fontes_fixas if f"eventdata_source_{s}" in df.columns]
    cols_outras = sorted([c for c in df.columns if c.startswith("eventdata_source_") and c not in set(cols_fixas)])

    cols_out = [c for c in cols_base if c in df.columns] + cols_fixas + cols_outras + \
               [c for c in df.columns if c not in set(cols_base + cols_fixas + cols_outras)]
    return df[cols_out].copy()

def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    return output.getvalue()

# =========================
# UI (Streamlit)
# =========================
def main():
    st.set_page_config(page_title=APP_NAME, page_icon="📚", layout="wide")
    inject_css()

    st.markdown(
        f"""
        <div class="hero">
          <div class="hero-title">📚 {APP_NAME}</div>
          <div class="hero-subtitle">{APP_SUBTITLE}</div>
          <div class="hero-caption">
            Interface orientada à extração reprodutível de indicadores bibliográficos e altmétricos
            (Crossref REST + Crossref Event Data), a partir de registros ORCID.
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.write("")

    # Sidebar: entradas e parâmetros
    with st.sidebar:
        st.markdown("### Parâmetros de execução")
        email = st.text_input(
            "E-mail (mailto) para as APIs",
            help="Usado como parâmetro 'mailto' em requisições Crossref. Preferencialmente institucional."
        )

        uploaded = st.file_uploader(
            "Upload do Excel com ORCIDs (.xlsx)",
            type=["xlsx"],
            help="A planilha deve conter uma coluna chamada 'orcid'. Caso não exista, será usada a primeira coluna."
        )

        st.markdown("### Controle de requisições")
        sleep_seconds = st.slider("Pausa entre requisições (s)", 0.0, 1.5, float(DEFAULT_SLEEP_SECONDS), 0.05)
        eventdata_rows = st.selectbox("Event Data: rows por página", [250, 500, 1000], index=2)
        eventdata_max_pages = st.slider("Event Data: limite de páginas", 1, 200, int(DEFAULT_EVENTDATA_MAX_PAGES), 1)

        st.markdown("### Fontes altmétricas (colunas fixas)")
        fontes_fixas = st.multiselect(
            "Selecione as fontes fixas (sempre presentes no Excel)",
            options=sorted(set(DEFAULT_FONTES_FIXAS)),
            default=DEFAULT_FONTES_FIXAS,
        )

        st.divider()
        executar = st.button("▶ Executar extração", type="primary", use_container_width=True)

    # Conteúdo principal
    colA, colB = st.columns([1.25, 1])

    with colA:
        st.markdown('<div class="card"><h3>Entradas e validação</h3></div>', unsafe_allow_html=True)
        st.write("")

        if uploaded is None:
            st.info("Envie um arquivo Excel (.xlsx) com a lista de ORCIDs para habilitar a execução.")
            return

        # Ler ORCIDs do Excel
        try:
            df_orcids = pd.read_excel(uploaded)
        except Exception as e:
            st.error(f"Não foi possível ler o Excel enviado. Detalhe: {e}")
            return

        orcid_col = "orcid" if "orcid" in [c.lower() for c in df_orcids.columns] else df_orcids.columns[0]

        # Ajuste: se a coluna for "ORCID" (maiúsculo), captura a coluna real
        if orcid_col != df_orcids.columns[0] and "orcid" in [c.lower() for c in df_orcids.columns]:
            for c in df_orcids.columns:
                if c.lower() == "orcid":
                    orcid_col = c
                    break

        raw_list = df_orcids[orcid_col].astype(str).tolist()
        orcids = []
        seen = set()
        invalid = []

        for x in raw_list:
            o = normalizar_orcid(x)
            if not o or o.lower() in {"nan", "none"}:
                continue
            if o not in seen:
                (orcids.append(o) if validar_orcid(o) else invalid.append(o))
                seen.add(o)

        # Bloco de status
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"<span class='badge'>Arquivo</span> {uploaded.name}", unsafe_allow_html=True)
        c2.markdown(f"<span class='badge'>ORCIDs válidos</span> {len(orcids)}", unsafe_allow_html=True)
        c3.markdown(f"<span class='badge'>Inválidos</span> {len(invalid)}", unsafe_allow_html=True)

        if invalid:
            with st.expander("Ver ORCIDs inválidos detectados"):
                st.write(invalid)

        st.write("")
        st.dataframe(df_orcids.head(20), use_container_width=True)

    with colB:
        st.markdown('<div class="card"><h3>Execução, logs e saída</h3></div>', unsafe_allow_html=True)
        st.write("")

        progress = st.progress(0)
        log_box = st.empty()

        logs = []

        def log_cb(msg: str):
            logs.append(msg)
            # Mostra somente últimas linhas para ficar limpo
            tail = "\n".join(logs[-40:])
            log_box.code(tail)

        def progress_cb(v: float):
            progress.progress(min(max(v, 0.0), 1.0))

        if executar:
            if not email:
                st.warning("Informe um e-mail para o parâmetro mailto (recomendado). A execução pode seguir sem ele, mas não é o ideal.")
            if not orcids:
                st.error("Nenhum ORCID válido foi identificado no arquivo enviado. Corrija a planilha e tente novamente.")
                return
            if not fontes_fixas:
                st.error("Selecione pelo menos uma fonte fixa.")
                return

            inicio = datetime.now()
            log_cb(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
            log_cb(f"ORCIDs válidos: {len(orcids)} | Fonte fixa(s): {len(fontes_fixas)}")

            try:
                df = coletar_para_lista_orcids(
                    orcids=orcids,
                    email=email.strip(),
                    fontes_fixas=fontes_fixas,
                    sleep_seconds=float(sleep_seconds),
                    eventdata_rows=int(eventdata_rows),
                    eventdata_max_pages=int(eventdata_max_pages),
                    progress_cb=progress_cb,
                    log_cb=log_cb
                )
            except Exception as e:
                st.error(f"Falha na execução do pipeline. Detalhe: {e}")
                return

            log_cb(f"Coleta finalizada. Linhas: {len(df)}")
            df_out = ordenar_colunas(df, fontes_fixas=fontes_fixas)

            st.success("Extração concluída com sucesso.")
            st.write("Prévia do dataset consolidado:")
            st.dataframe(df_out.head(50), use_container_width=True)

            # Gera Excel em memória
            output_name = f"orcid_crossref_eventdata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            excel_bytes = df_to_excel_bytes(df_out)

            st.download_button(
                label="⬇ Baixar Excel (aba: dados)",
                data=excel_bytes,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            fim = datetime.now()
            log_cb(f"Fim: {fim.strftime('%Y-%m-%d %H:%M:%S')}")
            log_cb(f"Duração: {str(fim - inicio)}")

    st.write("")
    with st.expander("Metadados e boas práticas"):
        st.markdown(
            """
- **Reprodutibilidade**: o app exporta uma única aba (`dados`) e mantém colunas fixas por fonte (mesmo com 0).
- **Crossref**: `is-referenced-by-count` representa citações dentro da cobertura Crossref.
- **Event Data**: contabiliza eventos por `source` (p.ex. twitter/news/blogs).  
- **Taxa de requisições**: ajuste “Pausa entre requisições” se houver instabilidade/rate limit.
            """
        )

if __name__ == "__main__":
    main()
