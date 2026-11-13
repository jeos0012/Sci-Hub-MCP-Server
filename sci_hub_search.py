"""Busca e download de artigos no Sci-Hub.

Camadas de transporte HTTP (em ordem de prioridade):

1. ``curl_cffi`` com ``impersonate="chrome"`` - resolve o fingerprint TLS,
   que o ``requests`` puro nao consegue imitar.
2. ``cloudscraper.create_scraper()`` - fallback quando o curl_cffi recebe
   um desafio (403/429/503, Cloudflare, captcha ou erro de requisicao).
3. ``DNSOverHTTPSAdapter`` - camada adicional de resolucao DNS via DoH,
   usada pelo fallback ``requests``/cloudscraper. O curl_cffi usa o
   ``doh_url`` nativo do libcurl.
"""

from scihub import SciHub
from bs4 import BeautifulSoup
import json
import logging
import os
import random
import re
import socket
import sys
import threading
import time
import urllib3
import requests

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from urllib.parse import quote, unquote, urljoin, urlparse
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError, NewConnectionError
from urllib3.util import connection as urllib3_connection
from requests.adapters import HTTPAdapter

try:  # Prioridade 1: fingerprint TLS real.
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - depende do ambiente
    curl_requests = None

try:  # Prioridade 2: fallback para desafios anti-bot.
    import cloudscraper
except ImportError:  # pragma: no cover - depende do ambiente
    cloudscraper = None

# 禁用 HTTPS 证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("scihub.transport")

DEFAULT_TIMEOUT = 30
DEFAULT_IMPERSONATE = os.environ.get("SCIHUB_IMPERSONATE", "chrome")
# Cloudflare DoH por IP: funciona mesmo quando o DNS do sistema esta bloqueado.
DEFAULT_DOH_URL = os.environ.get("SCIHUB_DOH_URL", "https://1.1.1.1/dns-query")
DOH_TIMEOUT = 5

# Status que podem melhorar em uma nova tentativa.
RETRYABLE_STATUS = {403, 408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
MAX_RETRY_DELAY = 30.0
MAX_BATCH_WORKERS = 4
DOI_HOSTS = {"doi.org", "dx.doi.org", "www.doi.org"}

# Mirrors atuais. A lista do pacote scihub 0.0.1 e de ~2016 e esta cheia de
# dominios mortos/parqueados, por isso ela nao e mais usada.
# Pode ser sobrescrita por SCIHUB_MIRRORS="host1,host2,...".
DEFAULT_SCIHUB_MIRRORS = (
    "sci-hub.ru",
    "sci-hub.ee",
    "sci-hub.ren",
    "sci-hub.usualwant.com",
    "sci-hub.st",
    "sci-hub.se",
    "sci-hub.wf",
)


def get_scihub_mirrors():
    """Retorna os mirrors configurados (env SCIHUB_MIRRORS ou padrao)."""
    raw = os.environ.get("SCIHUB_MIRRORS", "")
    hosts = [item.strip() for item in raw.split(",") if item.strip()]
    if not hosts:
        hosts = list(DEFAULT_SCIHUB_MIRRORS)

    mirrors = []
    for host in hosts:
        host = host.removeprefix("https://").removeprefix("http://").strip("/")
        if host and host not in mirrors:
            mirrors.append(host)
    return mirrors

# Status tipicos de bloqueio/desafio anti-bot.
CHALLENGE_STATUS = {403, 429, 503, 520, 521, 522, 523, 524}

# Marcadores encontrados em paginas de desafio (Cloudflare e similares).
CHALLENGE_MARKERS = (
    "just a moment",
    "cf-challenge",
    "cf_chl_opt",
    "challenge-platform",
    "attention required",
    "enable javascript and cookies",
    "checking your browser",
    "captcha",
)

# Opcoes exclusivas do curl_cffi que nao existem no requests/cloudscraper.
CURL_ONLY_KWARGS = {
    "impersonate",
    "ja3",
    "akamai",
    "perk",
    "extra_fp",
    "default_headers",
    "http_version",
    "doh_url",
    "dns",
    "interface",
    "content_callback",
    "max_recv_speed",
    "multipart",
    "discard_cookies",
    "quote",
    "default_encoding",
}


# ---------------------------------------------------------------------------
# DNS over HTTPS
# ---------------------------------------------------------------------------

@lru_cache(maxsize=512)
def resolve_via_doh(host, doh_url=DEFAULT_DOH_URL):
    """Resolve um hostname (registro A) via DNS over HTTPS.

    Retorna o primeiro IPv4 encontrado ou ``None``. O resultado fica em
    cache para evitar uma consulta DoH a cada nova conexao TCP.
    """
    if not host or host in ("localhost", "127.0.0.1", "::1"):
        return None
    # IPs literais nao precisam de resolucao.
    try:
        socket.inet_aton(host)
        return host
    except OSError:
        pass

    endpoints = []
    for candidate in (doh_url, "https://1.1.1.1/dns-query",
                      "https://8.8.8.8/resolve"):
        if candidate and candidate not in endpoints:
            endpoints.append(candidate)

    for endpoint in endpoints:
        try:
            response = requests.get(
                endpoint,
                params={"name": host, "type": "A"},
                headers={"accept": "application/dns-json"},
                timeout=DOH_TIMEOUT,
            )
            if response.status_code != 200:
                continue
            for answer in response.json().get("Answer", []):
                if answer.get("type") == 1 and answer.get("data"):
                    return answer["data"]
        except Exception as exc:  # DoH indisponivel: usa o DNS do sistema.
            logger.debug("DoH %s falhou para %s: %s", endpoint, host, exc)

    return None


class _DoHConnectionMixin:
    """Faz a conexao TCP usando o IP retornado pelo DoH.

    O hostname original permanece em ``self.host``, portanto o SNI e a
    validacao do certificado TLS continuam corretos.
    """

    _doh_url = DEFAULT_DOH_URL

    def _new_conn(self):
        try:
            resolved = resolve_via_doh(self.host, self._doh_url)
        except Exception:  # nunca impede a conexao normal
            resolved = None

        if not resolved:
            return super()._new_conn()

        # Conecta ao IP do DoH, mas mantem self.host (hostname original)
        # para que o SNI e a validacao do certificado TLS continuem corretos.
        try:
            return urllib3_connection.create_connection(
                (resolved, self.port),
                self.timeout,
                source_address=getattr(self, "source_address", None),
                socket_options=getattr(self, "socket_options", None),
            )
        except socket.timeout as exc:
            raise ConnectTimeoutError(
                self,
                f"Connection to {self.host} timed out. "
                f"(connect timeout={self.timeout})",
            ) from exc
        except OSError as exc:
            raise NewConnectionError(
                self, f"Failed to establish a new connection: {exc}"
            ) from exc


def _make_doh_pool_classes(doh_url):
    """Cria pools cujas conexoes resolvem via o DoH informado."""

    class DoHHTTPConnection(_DoHConnectionMixin, HTTPConnection):
        _doh_url = doh_url

    class DoHHTTPSConnection(_DoHConnectionMixin, HTTPSConnection):
        _doh_url = doh_url

    class DoHHTTPConnectionPool(HTTPConnectionPool):
        ConnectionCls = DoHHTTPConnection

    class DoHHTTPSConnectionPool(HTTPSConnectionPool):
        ConnectionCls = DoHHTTPSConnection

    return DoHHTTPConnectionPool, DoHHTTPSConnectionPool


class DNSOverHTTPSAdapter(HTTPAdapter):
    """``HTTPAdapter`` que resolve hostnames via DoH antes de conectar."""

    def __init__(self, doh_url=DEFAULT_DOH_URL, **kwargs):
        self.doh_url = doh_url
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        if hasattr(self.poolmanager, "pool_classes_by_scheme"):
            http_pool, https_pool = _make_doh_pool_classes(self.doh_url)
            self.poolmanager.pool_classes_by_scheme = {
                "http": http_pool,
                "https": https_pool,
            }


def _build_doh_requests_session():
    """Sessao ``requests`` com o adaptador DoH montado (fallback)."""
    session = requests.Session()
    adapter = DNSOverHTTPSAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# Sessao resiliente: curl_cffi -> cloudscraper
# ---------------------------------------------------------------------------

class ResilientSession:
    """Sessao compativel com ``requests``/``curl_cffi``.

    - ``get``/``request`` usam curl_cffi com ``impersonate="chrome"``;
    - em caso de erro de rede/TLS ou resposta de desafio, a mesma
      requisicao e repetida com ``cloudscraper``;
    - o fallback ``requests`` usa ``DNSOverHTTPSAdapter`` para contornar
      bloqueios de DNS.
    """

    def __init__(
        self,
        timeout=DEFAULT_TIMEOUT,
        impersonate=DEFAULT_IMPERSONATE,
        doh_url=DEFAULT_DOH_URL,
        verify=False,
        proxies=None,
    ):
        self.timeout = timeout
        self.impersonate = impersonate
        self.doh_url = doh_url
        self.verify = verify
        self.proxies = dict(proxies or {})
        self._headers = {}

        self._primary = self._build_primary()
        self._fallback = self._build_fallback()

        if self._fallback is None:
            logger.warning(
                "cloudscraper nao instalado: fallback anti-desafio desabilitado"
            )

    # -- construcao -----------------------------------------------------
    def _build_primary(self):
        if curl_requests is not None:
            logger.debug("Transporte primario: curl_cffi (%s)", self.impersonate)
            self._primary_is_curl = True
            return curl_requests.Session(
                impersonate=self.impersonate,
                timeout=self.timeout,
                verify=self.verify,
            )

        logger.warning(
            "curl_cffi nao instalado: usando requests com DNSOverHTTPSAdapter"
        )
        self._primary_is_curl = False
        return _build_doh_requests_session()

    def _build_fallback(self):
        if cloudscraper is None:
            return None
        try:
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
        except Exception as exc:  # pragma: no cover - versoes antigas
            logger.warning("Nao foi possivel criar o cloudscraper: %s", exc)
            return None

        # Camada adicional de DNS para o fallback requests.
        adapter = DNSOverHTTPSAdapter(doh_url=self.doh_url)
        scraper.mount("http://", adapter)
        scraper.mount("https://", adapter)
        if self.proxies:
            scraper.proxies.update(self.proxies)
        return scraper

    # -- propriedades compativeis com requests.Session -------------------
    @property
    def headers(self):
        return dict(self._headers)

    @headers.setter
    def headers(self, value):
        self._headers = dict(value or {})

    @property
    def cookies(self):
        return getattr(self._primary, "cookies", None)

    # -- requisicoes -----------------------------------------------------
    def request(self, method, url, **kwargs):
        """Executa a requisicao com curl_cffi e, se necessario, cloudscraper."""
        kwargs = dict(kwargs)
        timeout = kwargs.pop("timeout", None) or self.timeout
        stream = bool(kwargs.get("stream", False))

        try:
            response = self._primary_request(method, url, kwargs, timeout)
        except Exception as primary_error:
            logger.warning(
                "curl_cffi falhou em %s %s (%s); tentando cloudscraper",
                method,
                url,
                primary_error,
            )
            return self._fallback_request(
                method, url, kwargs, timeout, primary_error
            )

        if self._is_challenge(response, stream):
            logger.warning(
                "Desafio anti-bot detectado (HTTP %s) em %s; tentando cloudscraper",
                response.status_code,
                url,
            )
            response.close()
            return self._fallback_request(method, url, kwargs, timeout, None)

        return response

    def get(self, url, **kwargs):
        # Prioridade maxima: fingerprint TLS de Chrome em toda requisicao.
        kwargs["impersonate"] = self.impersonate
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        kwargs.setdefault("impersonate", self.impersonate)
        return self.request("POST", url, **kwargs)

    def _primary_request(self, method, url, kwargs, timeout):
        request_kwargs = {
            key: value for key, value in kwargs.items() if key not in ("proxy",)
        }
        request_kwargs["timeout"] = timeout
        request_kwargs["verify"] = request_kwargs.get("verify", self.verify)

        headers = dict(self._headers)
        headers.update(request_kwargs.pop("headers", None) or {})
        if headers:
            request_kwargs["headers"] = headers

        if self.proxies:
            proxies = dict(self.proxies)
            proxies.update(request_kwargs.get("proxies") or {})
            request_kwargs["proxies"] = proxies

        if self._primary_is_curl:
            request_kwargs["impersonate"] = request_kwargs.get(
                "impersonate", self.impersonate
            )
            if self.doh_url:
                request_kwargs.setdefault("doh_url", self.doh_url)
        else:
            # requests puro: remove opcoes exclusivas do curl.
            for key in CURL_ONLY_KWARGS:
                request_kwargs.pop(key, None)

        return self._primary.request(method, url, **request_kwargs)

    def _fallback_request(self, method, url, kwargs, timeout, primary_error):
        if self._fallback is None:
            if primary_error is not None:
                raise primary_error
            raise RuntimeError(
                "Requisicao bloqueada e cloudscraper indisponivel"
            )

        request_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key not in CURL_ONLY_KWARGS and key != "proxy"
        }
        request_kwargs["timeout"] = timeout
        # O CipherSuiteAdapter do cloudscraper reabilita check_hostname e
        # quebra com verify=False ("Cannot set verify_mode to CERT_NONE...").
        # Os mirrors atuais usam certificados validos, entao o fallback
        # sempre valida o TLS.
        request_kwargs["verify"] = True

        headers = dict(self._headers)
        headers.update(request_kwargs.pop("headers", None) or {})
        if headers:
            request_kwargs["headers"] = headers

        proxies = dict(self.proxies)
        proxies.update(request_kwargs.get("proxies") or {})
        if kwargs.get("proxy"):
            proxies.setdefault("http", kwargs["proxy"])
            proxies.setdefault("https", kwargs["proxy"])
        if proxies:
            request_kwargs["proxies"] = proxies

        try:
            return self._fallback.request(method, url, **request_kwargs)
        except Exception as fallback_error:
            logger.error("cloudscraper tambem falhou em %s: %s", url, fallback_error)
            if primary_error is not None:
                raise fallback_error from primary_error
            raise

    @staticmethod
    def _is_challenge(response, stream):
        if response.status_code in CHALLENGE_STATUS:
            return True

        headers = response.headers
        if (headers.get("cf-mitigated") or "").lower() == "challenge":
            return True
        if (headers.get("x-datadome") or "").lower() == "protected":
            return True

        content_type = (headers.get("Content-Type") or "").lower()
        if "application/pdf" in content_type or "octet-stream" in content_type:
            return False
        if "html" not in content_type:
            return False
        # Em streaming (download de PDF) uma pagina HTML ja e suspeita:
        # nao vale consumir o corpo, apenas tenta o cloudscraper.
        if stream:
            return True

        try:
            body = response.text.lower()
        except Exception:
            return False
        if not any(marker in body for marker in CHALLENGE_MARKERS):
            return False
        # Paginas reais do Sci-Hub podem citar "captcha" no JavaScript,
        # mas contem um link/objeto para o PDF. Nesse caso nao e desafio.
        return not re.search(r"(?:\.pdf|/storage/)", body)

    def close(self):
        for session in (self._primary, self._fallback):
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def create_resilient_session(**kwargs):
    """Cria a sessao resiliente (curl_cffi + cloudscraper + DoH)."""
    return ResilientSession(**kwargs)


def create_scihub_instance():
    """Cria um ``SciHub`` usando a sessao resiliente como transporte.

    O ``scihub`` instancia internamente um ``requests.Session``; ele e
    substituido por ``ResilientSession``, cuja prioridade e o curl_cffi
    com ``impersonate="chrome"``.
    """
    sh = SciHub()
    sh.timeout = DEFAULT_TIMEOUT  # 增加超时时间到 30 秒
    sh.available_base_url_list = get_scihub_mirrors()
    sh.session = create_resilient_session(timeout=DEFAULT_TIMEOUT)
    return sh


def _normalize_pdf_url(value, page_url):
    """Normaliza URLs absolutas, relativas e protocol-relative."""
    if not value:
        return None
    value = value.strip().strip("'\"")
    if value.startswith("//"):
        value = "https:" + value
    else:
        value = urljoin(page_url, value)
    value = value.split("#", 1)[0]
    if not value.lower().startswith(("http://", "https://")):
        return None
    return value


def _extract_pdf_candidates(html, page_url):
    """Extrai candidatos a PDF da pagina do Sci-Hub.

    O pacote ``scihub`` 0.0.1 so entende ``<iframe>``. Os mirrors atuais
    usam ``<object>``, ``<embed>``, ``citation_pdf_url`` e links diretos.
    """
    soup = BeautifulSoup(html, "html.parser")
    raw = []

    meta = soup.find("meta", attrs={"name": "citation_pdf_url"})
    if meta and meta.get("content"):
        raw.append(meta["content"])
    for tag_name, attr in (("iframe", "src"), ("object", "data"), ("embed", "src")):
        for node in soup.find_all(tag_name):
            value = node.get(attr)
            if value:
                raw.append(value)
    for node in soup.find_all("a", href=True):
        href = node["href"]
        if ".pdf" in href.lower() or "/storage/" in href.lower():
            raw.append(href)

    raw.extend(re.findall(r"""(?:https?:)?//[^"'\s<>]+\.pdf[^"'\s<>]*""", html, re.I))
    raw.extend(re.findall(r"""["'](/storage/[^"'\s<>]+\.pdf)[^"'\s<>]*""", html, re.I))

    candidates = []
    for value in raw:
        candidate = _normalize_pdf_url(value, page_url)
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _extract_metadata(html):
    """Le titulo/autores/ano dos metadados de citacao do Sci-Hub."""
    soup = BeautifulSoup(html, "html.parser")

    def meta_content(name):
        tag = soup.find("meta", attrs={"name": name})
        return (tag.get("content") or "").strip() if tag else ""

    title = meta_content("citation_title")
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
        title = re.sub(r"^Sci-Hub[.\s|:\-]*", "", title, flags=re.I)
        title = re.sub(r"\s*\|\s*10\.\d{4,9}/\S+\s*$", "", title)
        title = re.sub(r"\s*/\s*[^/]+,\s*\d{4}\s*$", "", title)

    authors = [
        (tag.get("content") or "").strip()
        for tag in soup.find_all("meta", attrs={"name": "citation_author"})
        if tag.get("content")
    ]
    year = meta_content("citation_publication_date") or meta_content("citation_year")
    if not year:
        match = re.search(r"\b(?:19|20)\d{2}\b", meta_content("citation_date"))
        year = match.group(0) if match else ""
    return title, ", ".join(authors), year


def find_pdf_in_html(html, base_url):
    """Compatibilidade: retorna (primeiro candidato a PDF, titulo)."""
    candidates = _extract_pdf_candidates(html, base_url)
    title, _, _ = _extract_metadata(html)
    return (candidates[0] if candidates else None), title


def _verify_pdf_url(session, pdf_url, referer):
    """Confirma que a URL realmente devolve um PDF (e nao um anuncio)."""
    response = session.get(
        pdf_url,
        timeout=DEFAULT_TIMEOUT,
        stream=True,
        allow_redirects=True,
        headers={"Referer": referer},
    )
    try:
        if response.status_code != 200:
            return False
        content_type = (response.headers.get("Content-Type") or "").lower()
        first_chunk = b""
        for chunk in response.iter_content(chunk_size=1024):
            first_chunk = chunk or b""
            break
        return (
            "application/pdf" in content_type
            or first_chunk.lstrip().startswith(b"%PDF")
        )
    finally:
        response.close()


def _resolve_pdf(doi, session):
    """Percorre os mirrors e retorna o primeiro PDF valido encontrado."""
    errors = []
    for host in get_scihub_mirrors():
        page_url = f"https://{host}/{quote(doi, safe='/')}"
        try:
            response = session.get(
                page_url,
                timeout=DEFAULT_TIMEOUT,
                headers={"Referer": f"https://{host}/"},
            )
        except Exception as exc:
            errors.append(f"{host}: {type(exc).__name__}")
            continue

        try:
            if response.status_code != 200:
                errors.append(f"{host}: HTTP {response.status_code}")
                continue

            content_type = (response.headers.get("Content-Type") or "").lower()
            if "html" not in content_type:
                errors.append(
                    f"{host}: content-type inesperado ({content_type or '?'})"
                )
                continue

            html = response.text
            candidates = _extract_pdf_candidates(html, page_url)
            if not candidates:
                errors.append(f"{host}: pagina sem link de PDF (possivel desafio)")
                continue

            for candidate in candidates:
                try:
                    if _verify_pdf_url(session, candidate, page_url):
                        title, author, year = _extract_metadata(html)
                        return {
                            "pdf_url": candidate,
                            "title": title,
                            "author": author,
                            "year": year,
                            "mirror": host,
                        }, errors
                except Exception as exc:
                    errors.append(f"{host}: PDF {type(exc).__name__}")
            errors.append(f"{host}: links encontrados nao retornaram PDF")
        except Exception as exc:
            errors.append(f"{host}: {type(exc).__name__}")
        finally:
            response.close()

    return None, errors


def _crossref_metadata(doi):
    """Completa metadados ausentes usando a API do CrossRef."""
    try:
        response = requests.get(
            f"https://api.crossref.org/works/{quote(doi, safe='/')}",
            timeout=10,
        )
        if response.status_code != 200:
            return "", "", ""
        item = response.json().get("message", {})
        title = (item.get("title") or [""])[0]
        authors = []
        for author in item.get("author", []):
            name = " ".join(
                part for part in (author.get("given"), author.get("family")) if part
            )
            if name:
                authors.append(name)
        issued = item.get("issued", {}).get("date-parts", [[]])
        year = str(issued[0][0]) if issued and issued[0] else ""
        return title, ", ".join(authors), year
    except Exception:
        return "", "", ""


def search_paper_by_doi(doi, session=None):
    """通过 DOI 在 Sci-Hub 上搜索论文"""
    owns_session = session is None
    session = session or create_resilient_session()
    try:
        resolved, errors = _resolve_pdf(doi, session)
        if resolved:
            # Alguns mirrors (ex.: sci-hub.ee) nao trazem autores/ano.
            if not resolved.get("author") or not resolved.get("year"):
                cr_title, cr_author, cr_year = _crossref_metadata(doi)
                resolved["title"] = resolved.get("title") or cr_title
                resolved["author"] = resolved.get("author") or cr_author
                resolved["year"] = resolved.get("year") or cr_year
            return {'doi': doi, 'status': 'success', **resolved}

        reason = "; ".join(errors[:8]) or "nenhum mirror configurado"
        print(
            f"搜索出错: nenhum mirror retornou PDF para {doi} ({reason})",
            file=sys.stderr,
        )
        return {
            'doi': doi,
            'status': 'not_found',
            'error': reason,
        }
    except Exception as e:
        print(f"搜索出错: {str(e)}", file=sys.stderr)
        return {
            'doi': doi,
            'status': 'not_found',
            'error': str(e),
        }
    finally:
        if owns_session:
            session.close()


def search_paper_by_title(title):
    """通过标题在 Sci-Hub 上搜索论文"""
    # 由于 SciHub 包不支持 search 方法，我们改用 DOI 搜索
    # 首先尝试从 CrossRef 获取 DOI
    try:
        url = f"https://api.crossref.org/works?query.title={title}&rows=1"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data['message']['items']:
                doi = data['message']['items'][0]['DOI']
                return search_paper_by_doi(doi)
    except Exception as e:
        print(f"CrossRef 搜索出错: {str(e)}", file=sys.stderr)
    
    return {
        'title': title,
        'status': 'not_found'
    }


def search_papers_by_keyword(keyword, num_results=10):
    """通过关键词搜索论文，返回元数据列表"""
    # 使用 CrossRef API 进行搜索
    papers = []
    try:
        url = f"https://api.crossref.org/works?query={keyword}&rows={num_results}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            for item in data['message']['items']:
                doi = item.get('DOI')
                if doi:
                    result = search_paper_by_doi(doi)
                    if result['status'] == 'success':
                        papers.append(result)
    except Exception as e:
        print(f"搜索出错: {str(e)}", file=sys.stderr)
    
    return papers


def _looks_like_pdf(path):
    try:
        with open(path, "rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


@dataclass
class _DownloadOutcome:
    ok: bool
    reason: str = ""
    retryable: bool = False
    retry_after: float | None = None
    size: int = 0
    content_type: str = ""


def _remove_file(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def _retry_after_seconds(response):
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def _sleep_before_retry(attempt, backoff, retry_after=None):
    """Espera exponencial com jitter; respeita Retry-After quando presente."""
    if retry_after is not None:
        delay = min(float(retry_after), MAX_RETRY_DELAY)
    else:
        delay = min(max(0.0, backoff) * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
    if delay > 0:
        time.sleep(delay * random.uniform(0.85, 1.15))


def _download_once(session, pdf_url, temp_path):
    """Faz uma unica tentativa de download e valida o conteudo."""
    response = session.get(
        pdf_url,
        timeout=DEFAULT_TIMEOUT,
        stream=True,
        allow_redirects=True,
    )
    try:
        status = response.status_code
        content_type = (response.headers.get("Content-Type") or "").lower()

        if not 200 <= status < 300:
            return _DownloadOutcome(
                False,
                f"HTTP {status}",
                status in RETRYABLE_STATUS,
                _retry_after_seconds(response),
                content_type=content_type,
            )

        if "html" in content_type:
            return _DownloadOutcome(
                False,
                f"servidor retornou {content_type} em vez de PDF",
                True,
                content_type=content_type,
            )

        size = 0
        with open(temp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
                    size += len(chunk)

        if size == 0:
            return _DownloadOutcome(
                False, "resposta vazia", True, content_type=content_type
            )
        if not _looks_like_pdf(temp_path):
            return _DownloadOutcome(
                False,
                "conteudo baixado nao e um PDF valido",
                True,
                content_type=content_type,
            )
        return _DownloadOutcome(True, size=size, content_type=content_type)
    finally:
        response.close()


def _resolve_page_pdf(session, page_url):
    """Extrai e valida um PDF a partir de uma pagina HTML (ex.: Sci-Hub)."""
    response = session.get(
        page_url,
        timeout=DEFAULT_TIMEOUT,
        headers={"Referer": page_url},
    )
    try:
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"

        content_type = (response.headers.get("Content-Type") or "").lower()
        if "html" not in content_type:
            return None, f"content-type inesperado ({content_type or '?'})"

        html = response.text
        candidates = _extract_pdf_candidates(html, page_url)
        if not candidates:
            return None, "pagina sem link de PDF (possivel desafio)"

        for candidate in candidates:
            try:
                if _verify_pdf_url(session, candidate, page_url):
                    title, author, year = _extract_metadata(html)
                    return {
                        "pdf_url": candidate,
                        "title": title,
                        "author": author,
                        "year": year,
                        "mirror": urlparse(page_url).netloc,
                    }, None
            except Exception:
                continue
        return None, "links encontrados nao retornaram PDF"
    finally:
        response.close()


def _resolve_doi(doi, session):
    result = search_paper_by_doi(doi, session=session)
    if result.get("status") == "success":
        return {
            key: result.get(key, "")
            for key in ("pdf_url", "title", "author", "year", "mirror")
        }, None
    return None, result.get("error") or "DOI nao encontrado"


def _resolve_download_target(session, target):
    """Resolve DOI, URL de PDF, URL de pagina ou URL doi.org para um PDF."""
    value = (target or "").strip()
    if not value:
        return None, "entrada vazia"

    if value.lower().startswith(("http://", "https://")):
        parsed = urlparse(value)
        if parsed.netloc.lower() in DOI_HOSTS:
            doi = unquote(parsed.path).lstrip("/")
            return _resolve_doi(doi, session)
        if parsed.path.lower().endswith(".pdf"):
            return {
                "pdf_url": value,
                "title": "",
                "author": "",
                "year": "",
                "mirror": parsed.netloc,
            }, None
        return _resolve_page_pdf(session, value)

    return _resolve_doi(value, session)


def _safe_filename(value, fallback="paper.pdf"):
    """Sanitiza nome de arquivo para Windows/Linux e garante extensao .pdf."""
    value = unquote(str(value or "")).strip()
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value)
    value = re.sub(r"\s+", " ", value).strip().strip(".")
    if not value:
        value = fallback
    if not value.lower().endswith(".pdf"):
        value += ".pdf"

    stem, ext = os.path.splitext(value)
    if stem.upper() in {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    }:
        stem = "_" + stem
    stem = stem[: max(1, 120 - len(ext))] or "paper"
    return f"{stem}{ext}"


def _filename_for_item(item, index):
    fallback = f"paper_{index + 1:03d}.pdf"
    value = (item or "").strip()
    if not value:
        return fallback

    if value.lower().startswith(("http://", "https://")):
        parsed = urlparse(value)
        if parsed.netloc.lower() in DOI_HOSTS:
            value = unquote(parsed.path).lstrip("/")
        else:
            match = re.search(r"(10\.\d{4,9}/[^\s?#]+)", unquote(parsed.path))
            if match:
                value = match.group(1)
            else:
                value = os.path.basename(unquote(parsed.path))
    else:
        value = value.replace("/", "_")

    return _safe_filename(value, fallback)


def _reserve_filename(base_name, reserved, lock):
    with lock:
        name = base_name
        if name in reserved:
            stem, ext = os.path.splitext(base_name)
            counter = 2
            while f"{stem}_{counter}{ext}" in reserved:
                counter += 1
            name = f"{stem}_{counter}{ext}"
        reserved.add(name)
        return name


def download_paper(pdf_url, output_path, retries=2, backoff=2.0, session=None):
    """Baixa um PDF de uma URL, DOI ou pagina do Sci-Hub, com retentativas.

    ``retries`` e o numero de tentativas adicionais (total = retries + 1).
    Aceita URL direta de PDF, DOI (ex.: ``10.1038/171737a0``), URL do
    doi.org ou pagina HTML de um mirror (extrai o PDF da pagina).
    """
    retries = max(0, int(retries))
    backoff = max(0.0, float(backoff))
    owns_session = session is None
    session = session or create_resilient_session()

    output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    temp_path = f"{output_path}.part"
    last_reason = ""

    try:
        for attempt in range(1, retries + 2):
            try:
                resolved, resolve_error = _resolve_download_target(session, pdf_url)
                if resolved is None:
                    outcome = _DownloadOutcome(
                        False, resolve_error or "alvo nao resolvido", True
                    )
                else:
                    outcome = _download_once(session, resolved["pdf_url"], temp_path)
            except Exception as exc:
                outcome = _DownloadOutcome(
                    False, f"{type(exc).__name__}: {exc}", True
                )

            if outcome.ok:
                try:
                    os.replace(temp_path, output_path)
                    return True
                except OSError as exc:
                    last_reason = f"falha ao salvar: {exc}"
                    break

            last_reason = outcome.reason
            _remove_file(temp_path)
            if not outcome.retryable or attempt > retries:
                break
            logger.warning(
                "Download falhou (tentativa %s/%s) para %s: %s",
                attempt,
                retries + 1,
                pdf_url,
                last_reason,
            )
            _sleep_before_retry(attempt, backoff, outcome.retry_after)

        print(f"下载出错: {last_reason or 'falha desconhecida'}", file=sys.stderr)
        return False
    finally:
        if owns_session:
            session.close()
        _remove_file(temp_path)


def download_papers_batch(
    items,
    output_dir,
    retries=2,
    concurrency=2,
    overwrite=False,
    backoff=2.0,
):
    """Baixa varios DOIs/URLs em paralelo, com retry e manifesto JSON.

    Cada item pode ser um DOI, URL de PDF, URL doi.org ou pagina de mirror.
    Arquivos existentes e validos sao pulados quando ``overwrite=False``.
    """
    if isinstance(items, str):
        items = [items]

    raw_items = [
        str(item).strip() for item in (items or []) if str(item).strip()
    ]
    unique_items = list(dict.fromkeys(raw_items))

    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    retries = max(0, int(retries))
    backoff = max(0.0, float(backoff))
    workers = max(1, min(int(concurrency or 1), MAX_BATCH_WORKERS))

    if not unique_items:
        return {
            "output_dir": output_dir,
            "manifest": None,
            "total": 0,
            "success": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        }

    reserved = set()
    lock = threading.Lock()
    thread_local = threading.local()
    sessions = []

    def get_session():
        session = getattr(thread_local, "session", None)
        if session is None:
            session = create_resilient_session()
            thread_local.session = session
            with lock:
                sessions.append(session)
        return session

    def worker(pair):
        index, item = pair
        session = get_session()

        base_name = _filename_for_item(item, index)
        name = _reserve_filename(base_name, reserved, lock)
        final_path = os.path.join(output_dir, name)

        if (
            not overwrite
            and os.path.exists(final_path)
            and _looks_like_pdf(final_path)
        ):
            return {
                "input": item,
                "status": "skipped",
                "path": final_path,
                "size": os.path.getsize(final_path),
                "attempts": 0,
                "pdf_url": "",
                "title": "",
                "error": "",
            }

        temp_path = f"{final_path}.part"
        last_reason = ""
        attempts = 0
        resolved = None

        for attempt in range(1, retries + 2):
            attempts = attempt
            try:
                resolved, resolve_error = _resolve_download_target(session, item)
                if resolved is None:
                    outcome = _DownloadOutcome(
                        False, resolve_error or "alvo nao resolvido", True
                    )
                else:
                    outcome = _download_once(session, resolved["pdf_url"], temp_path)
            except Exception as exc:
                outcome = _DownloadOutcome(
                    False, f"{type(exc).__name__}: {exc}", True
                )

            if outcome.ok:
                try:
                    os.replace(temp_path, final_path)
                except OSError as exc:
                    last_reason = f"falha ao salvar: {exc}"
                    break
                return {
                    "input": item,
                    "status": "success",
                    "path": final_path,
                    "size": os.path.getsize(final_path),
                    "attempts": attempt,
                    "pdf_url": (resolved or {}).get("pdf_url", ""),
                    "title": (resolved or {}).get("title", ""),
                    "error": "",
                }

            last_reason = outcome.reason
            _remove_file(temp_path)
            if not outcome.retryable or attempt > retries:
                break
            logger.warning(
                "Lote: tentativa %s/%s falhou para %s: %s",
                attempt,
                retries + 1,
                item,
                last_reason,
            )
            _sleep_before_retry(attempt, backoff, outcome.retry_after)

        _remove_file(temp_path)
        return {
            "input": item,
            "status": "failed",
            "path": None,
            "size": 0,
            "attempts": attempts,
            "pdf_url": (resolved or {}).get("pdf_url", ""),
            "title": (resolved or {}).get("title", ""),
            "error": last_reason or "falha desconhecida",
        }

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(worker, enumerate(unique_items)))
    finally:
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass

    summary = {
        "success": sum(1 for item in results if item["status"] == "success"),
        "skipped": sum(1 for item in results if item["status"] == "skipped"),
        "failed": sum(1 for item in results if item["status"] == "failed"),
    }
    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": output_dir,
        "retries": retries,
        "concurrency": workers,
        "overwrite": bool(overwrite),
        "total": len(results),
        **summary,
        "results": results,
    }
    manifest_path = os.path.join(
        output_dir,
        f"download_manifest_{datetime.now():%Y%m%d_%H%M%S_%f}.json",
    )
    try:
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.error("Nao foi possivel gravar o manifesto: %s", exc)
        manifest_path = None

    return {
        "output_dir": output_dir,
        "manifest": manifest_path,
        "total": len(results),
        **summary,
        "results": results,
    }


if __name__ == "__main__":
    print("Sci-Hub 论文搜索测试\n")

    # 1. DOI 搜索测试
    print("1. 通过 DOI 搜索论文")
    test_doi = "10.1002/jcad.12075"  # 一篇神经科学相关的论文
    result = search_paper_by_doi(test_doi)
    
    if result['status'] == 'success':
        print(f"标题: {result['title']}")
        print(f"作者: {result['author']}")
        print(f"年份: {result['year']}")
        print(f"PDF URL: {result['pdf_url']}")
        
        # 尝试下载论文
        output_file = f"paper_{test_doi.replace('/', '_')}.pdf"
        if download_paper(result['pdf_url'], output_file):
            print(f"论文已下载到: {output_file}")
        else:
            print("论文下载失败")
    else:
        print(f"未找到 DOI 为 {test_doi} 的论文")

    # 2. 标题搜索测试
    print("\n2. 通过标题搜索论文")
    test_title = "Choosing Assessment Instruments for Posttraumatic Stress Disorder Screening and Outcome Research"
    result = search_paper_by_title(test_title)
    
    if result['status'] == 'success':
        print(f"DOI: {result['doi']}")
        print(f"作者: {result['author']}")
        print(f"年份: {result['year']}")
        print(f"PDF URL: {result['pdf_url']}")
    else:
        print(f"未找到标题为 '{test_title}' 的论文")

    # 3. 关键词搜索测试
    print("\n3. 通过关键词搜索论文")
    test_keyword = "artificial intelligence medicine 2023"
    papers = search_papers_by_keyword(test_keyword, num_results=3)
    
    for i, paper in enumerate(papers, 1):
        print(f"\n论文 {i}:")
        print(f"标题: {paper['title']}")
        print(f"DOI: {paper['doi']}")
        print(f"作者: {paper['author']}")
        print(f"年份: {paper['year']}")
        if paper.get('pdf_url'):
            print(f"PDF URL: {paper['pdf_url']}")
