# Sci-Hub MCP Server

[![smithery badge](https://smithery.ai/badge/@JackKuo666/sci-hub-mcp-server)](https://smithery.ai/server/@JackKuo666/sci-hub-mcp-server)

🔍 Enable AI assistants to search, access, and analyze academic papers through Sci-Hub using a simple MCP interface.

The Sci-Hub MCP Server provides a bridge between AI assistants and Sci-Hub's repository of academic literature through the Model Context Protocol (MCP). It allows AI models to search for scientific articles by DOI, title, or keywords, access their metadata, and download PDFs in a programmatic way.

## ✨ Core Features

- 🔎 Paper Search by DOI: Find papers using their Digital Object Identifier ✅
- 🔍 Paper Search by Title: Locate papers using their full or partial title ✅
- 🔑 Paper Search by Keyword: Discover papers related to specific research areas ✅
- 📊 Metadata Access: Retrieve detailed metadata for specific papers ✅
- 📄 PDF Download: Download full-text PDF content when available ✅

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- FastMCP library

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/JackKuo666/Sci-Hub-MCP-Server.git
   cd Sci-Hub-MCP-Server
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## 📊 Usage

Start the MCP server:

```bash
python sci_hub_server.py
```

## Usage with Claude Desktop

Add this configuration to your `claude_desktop_config.json`:

(Mac OS)

```json
{
  "mcpServers": {
    "scihub": {
      "command": "python",
      "args": ["-m", "sci_hub_server.py"]
      }
  }
}
```

(Windows version):

```json
{
  "mcpServers": {
    "scihub": {
      "command": "C:\\Users\\YOUR\\PATH\\miniconda3\\envs\\mcp_server\\python.exe",
      "args": [
        "D:\\code\\YOUR\\PATH\\Sci-Hub-MCP-Server\\sci_hub_server.py"
      ],
      "env": {},
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## 🛠 MCP Tools

The Sci-Hub MCP Server provides the following tools:

1. `search_scihub_by_doi`: Search for a paper on Sci-Hub using its DOI (Digital Object Identifier).
2. `search_scihub_by_title`: Search for a paper on Sci-Hub using its title.
3. `search_scihub_by_keyword`: Search for papers on Sci-Hub using a keyword.
4. `download_scihub_pdf`: Download one paper PDF (accepts a PDF URL, a DOI, a doi.org URL or a Sci-Hub page URL) with optional retries.
5. `download_scihub_pdfs_batch`: Download several papers at once with per-item retries, concurrency and a JSON manifest.
6. `get_paper_metadata`: Get metadata information for a paper using its DOI.

### Searching Papers by DOI

You can ask the AI assistant to search for papers using DOI:
```
Can you search Sci-Hub for the paper with DOI 10.1038/nature09492?
```

### Searching Papers by Title

You can search for papers using their title:
```
Can you find the paper titled "Choosing Assessment Instruments for Posttraumatic Stress Disorder Screening and Outcome Research" on Sci-Hub?
```

### Searching Papers by Keyword

You can search for papers related to specific keywords:
```
Can you search Sci-Hub for recent papers about artificial intelligence in medicine?
```

### Downloading Papers

Once you have found a paper, you can download it:
```
Can you download the PDF for this paper to my_paper.pdf?
```

`download_scihub_pdf` also accepts a DOI directly, so you do not need a search
result first:

```
Download 10.1038/171737a0 to watson1953.pdf
```

Targets accepted by both download tools:

- a DOI: `10.1038/171737a0`
- a doi.org URL: `https://doi.org/10.1038/171737a0`
- a direct PDF URL: `https://.../watson1953.pdf`
- a Sci-Hub mirror page: `https://sci-hub.ru/10.1038/171737a0` (the PDF link is
  extracted from the page)

#### Retries

`download_scihub_pdf` has an optional `retries` argument (default `2`, i.e. up to
3 attempts). Only transient failures are retried: timeouts, network errors,
HTTP 403/408/425/429/5xx, empty responses, HTML challenge pages and bodies that
are not valid PDFs. A permanent failure such as HTTP 404 fails immediately.
Waits are exponential with jitter and honour the `Retry-After` header.

### Batch Downloading

`download_scihub_pdfs_batch` downloads a list of DOIs/URLs into a directory:

```json
{
  "items": ["10.1038/171737a0", "https://doi.org/10.1038/nature09492"],
  "output_dir": "C:\\papers",
  "retries": 2,
  "concurrency": 2,
  "overwrite": false
}
```

Behavior:

- **Inputs**: DOIs, PDF URLs, doi.org URLs and Sci-Hub mirror pages, mixed freely.
- **Naming**: filenames are derived from the DOI/URL and sanitized for the
  filesystem (Windows reserved names included). Collisions get `_2`, `_3` suffixes.
- **Resumable**: with `overwrite=false` (default), an existing valid PDF with the
  same derived filename is skipped, so a re-run only fetches what is missing.
- **Atomic writes**: data goes to `<name>.pdf.part` and is moved into place with
  `os.replace` only after the `%PDF` check passes.
- **Concurrency**: `concurrency` is clamped to 1–4 workers, each with its own
  resilient session; keep it low to reduce mirror blocking.
- **Manifest**: every run writes `download_manifest_<timestamp>.json` with the
  parameters and a per-item record (`input`, `status`, `path`, `size`,
  `attempts`, `pdf_url`, `title`, `error`).
- **Statuses**: `success`, `skipped` (already present) and `failed`.

The result contains `output_dir`, `manifest`, `total`, `success`, `skipped`,
`failed` and `results`.

### Environment Variables

- `SCIHUB_MIRRORS`: comma-separated mirror hosts, e.g.
  `sci-hub.ru,sci-hub.ee,sci-hub.ren`. Defaults to a maintained list.
- `SCIHUB_IMPERSONATE`: TLS fingerprint used by curl_cffi (default `chrome`).
- `SCIHUB_DOH_URL`: DNS-over-HTTPS endpoint (default `https://1.1.1.1/dns-query`).

> After changing the code, restart the MCP server (or reconnect the client) so the
> running process picks up the new tools.

### Getting Paper Metadata

You can request metadata for a paper using its DOI:
```
Can you show me the metadata for the paper with DOI 10.1038/nature09492?
```

## 📁 Project Structure

- `sci_hub_server.py`: The main MCP server implementation using FastMCP
- `sci_hub_search.py`: Contains the logic for searching Sci-Hub and retrieving paper information

## 🔧 Dependencies

- Python 3.10+
- FastMCP
- requests
- bs4 / beautifulsoup4
- scihub
- curl_cffi (TLS fingerprint, primary transport)
- cloudscraper (anti-bot fallback)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.

## ⚠️ Disclaimer

This tool is for research purposes only. Please respect copyright laws and use this tool responsibly. The authors do not endorse or encourage any copyright infringement.
