"""Discover direct passenger-car workbook links on the official source page."""

from __future__ import annotations

import hashlib
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse


SOURCE_PAGE_URL = (
    "https://www.vero.fi/henkiloasiakkaat/omaisuus/autovero/"
    "autoveron-maara/ajoneuvojen-sovellettuja-verotusarvoja/"
)
_ALLOWED_HOSTS = {"vero.fi", "www.vero.fi"}
_YEAR_PATTERN = re.compile(r"\b(20\d{2})\b")
_MAX_PAGE_BYTES = 2 * 1024 * 1024
_MAX_WORKBOOK_BYTES = 50 * 1024 * 1024
_USER_AGENT = "used-car-tax-values-demo/0.1"


@dataclass(frozen=True)
class WorkbookLink:
    decision_year: int
    label: str
    url: str


@dataclass(frozen=True)
class DownloadedWorkbook:
    link: WorkbookLink
    content: bytes
    sha256: str
    retrieved_at_utc: str
    content_type: str | None
    etag: str | None
    last_modified: str | None


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        self._href = dict(attrs).get("href")
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, " ".join(" ".join(self._text).split())))
            self._href = None
            self._text = []


def discover_latest_passenger_workbook(
    html: str, source_page_url: str = SOURCE_PAGE_URL
) -> WorkbookLink:
    """Return the newest linked passenger-car XLS; reject non-Vero hosts."""
    parser = _AnchorParser()
    parser.feed(html)
    candidates: list[WorkbookLink] = []

    for href, label in parser.links:
        url = urljoin(source_page_url, href)
        parsed_url = urlparse(url)
        if parsed_url.scheme != "https" or parsed_url.hostname not in _ALLOWED_HOSTS:
            continue
        if not parsed_url.path.lower().endswith(".xls"):
            continue
        if "henkilöauto" not in label.casefold() and "henkiloauto" not in parsed_url.path.casefold():
            continue
        year_match = _YEAR_PATTERN.search(f"{label} {parsed_url.path}")
        if not year_match:
            continue
        candidates.append(
            WorkbookLink(int(year_match.group(1)), label, url)
        )

    if not candidates:
        raise ValueError("No passenger-car XLS link found on the source page.")
    return max(candidates, key=lambda link: link.decision_year)


def _get_bytes(url: str, max_bytes: int, accept: str) -> tuple[bytes, Any]:
    parsed_url = urlparse(url)
    if parsed_url.scheme != "https" or parsed_url.hostname not in _ALLOWED_HOSTS:
        raise ValueError("Refusing to fetch a URL outside the official HTTPS Vero host.")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": _USER_AGENT, "Accept": accept},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        final_url = urlparse(response.geturl())
        if final_url.scheme != "https" or final_url.hostname not in _ALLOWED_HOSTS:
            raise ValueError("Refusing to follow a redirect outside the official HTTPS Vero host.")
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            raise ValueError("Source response exceeds the configured size limit.")
        content = response.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise ValueError("Source response exceeds the configured size limit.")
        return content, response.headers


def download_latest_workbook(
    source_page_url: str = SOURCE_PAGE_URL,
) -> DownloadedWorkbook:
    """Fetch the source page and its newest passenger-car XLS into memory."""
    page_content, _ = _get_bytes(
        source_page_url, _MAX_PAGE_BYTES, "text/html,application/xhtml+xml"
    )
    page_html = page_content.decode("utf-8", errors="replace")
    link = discover_latest_passenger_workbook(page_html, source_page_url)
    content, headers = _get_bytes(link.url, _MAX_WORKBOOK_BYTES, "application/vnd.ms-excel,*/*")
    return DownloadedWorkbook(
        link=link,
        content=content,
        sha256=hashlib.sha256(content).hexdigest(),
        retrieved_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        content_type=headers.get("Content-Type"),
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    )
