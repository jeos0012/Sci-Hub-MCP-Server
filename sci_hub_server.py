from typing import Any, List, Dict, Optional, Union
import asyncio
import logging
from mcp.server.fastmcp import FastMCP
from sci_hub_search import (
    search_paper_by_doi,
    search_paper_by_title,
    search_papers_by_keyword,
    download_paper,
    download_papers_batch,
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize FastMCP server
mcp = FastMCP("scihub")

@mcp.tool()
async def search_scihub_by_doi(doi: str) -> Dict[str, Any]:
    logging.info(f"Searching for paper with DOI: {doi}")
    """
    Search for a paper on Sci-Hub using its DOI (Digital Object Identifier).

    Args:
        doi (str): The Digital Object Identifier of the paper, a unique alphanumeric string 
             that identifies academic, professional, and scientific content 
             (e.g., "10.1038/nature09492").

    Returns:
        Dict[str, Any]: Dictionary containing paper information including:
            - title: The title of the paper
            - author: The author(s) of the paper
            - year: Publication year
            - pdf_url: URL to download the PDF if available
            - status: Success or error status
            - error: Error message if search failed
    """
    try:
        result = await asyncio.to_thread(search_paper_by_doi, doi)
        return result
    except Exception as e:
        return {"error": f"An error occurred while searching by DOI: {str(e)}"}

@mcp.tool()
async def search_scihub_by_title(title: str) -> Dict[str, Any]:
    logging.info(f"Searching for paper with title: {title}")
    """
    Search for a paper on Sci-Hub using its title.

    Args:
        title (str): The full or partial title of the academic paper to search for.
               More specific and complete titles will yield more accurate results.

    Returns:
        Dict[str, Any]: Dictionary containing paper information including:
            - title: The title of the paper
            - author: The author(s) of the paper
            - year: Publication year
            - pdf_url: URL to download the PDF if available
            - status: Success or error status
            - error: Error message if search failed
    """
    try:
        result = await asyncio.to_thread(search_paper_by_title, title)
        return result
    except Exception as e:
        return {"error": f"An error occurred while searching by title: {str(e)}"}

@mcp.tool()
async def search_scihub_by_keyword(keyword: str, num_results: int = 10) -> List[Dict[str, Any]]:
    logging.info(f"Searching for papers with keyword: {keyword}, number of results: {num_results}")
    """
    Search for papers on Sci-Hub using a keyword.

    Args:
        keyword (str): The keyword or search term to use for finding relevant papers.
                 Can be a subject, concept, or any term related to the research area.
        num_results (int, optional): Maximum number of search results to return. 
                      Defaults to 10. Higher values may increase search time.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing information about a paper:
            - title: The title of the paper
            - author: The author(s) of the paper
            - year: Publication year
            - doi: Digital Object Identifier if available
            - pdf_url: URL to download the PDF if available
            - status: Success or error status
            - error: Error message if search failed
    """
    try:
        results = await asyncio.to_thread(search_papers_by_keyword, keyword, num_results)
        return results
    except Exception as e:
        return [{"error": f"An error occurred while searching by keyword: {str(e)}"}]

@mcp.tool()
async def download_scihub_pdf(pdf_url: str, output_path: str, retries: int = 2) -> str:
    logging.info(f"Attempting to download PDF from {pdf_url} to {output_path} (retries={retries})")
    """
    Download a paper PDF from Sci-Hub.

    Args:
        pdf_url (str): The target to download. Accepts a direct link to the PDF file
                 (typically from a previous search result's 'pdf_url' field), a DOI
                 (e.g. "10.1038/171737a0"), a doi.org URL, or a Sci-Hub mirror page URL
                 (the PDF link is extracted from the page).
        output_path (str): The file path where the downloaded PDF should be saved.
                     Should include the filename with .pdf extension.
        retries (int, optional): Number of extra attempts after the first one when the
                     failure is transient (timeout, 5xx, 429, HTML challenge page).
                     Defaults to 2 (3 attempts total). Use 0 to disable retries.

    Returns:
        str: A message indicating the download result:
             - Success message with the output path if download was successful
             - Failure message if download failed
             - Error message with exception details if an error occurred
    """
    try:
        success = await asyncio.to_thread(
            download_paper, pdf_url, output_path, retries=retries
        )
        if success:
            return f"PDF successfully downloaded to {output_path}"
        else:
            return f"Failed to download PDF to {output_path}"
    except Exception as e:
        return f"An error occurred while downloading PDF: {str(e)}"

@mcp.tool()
async def download_scihub_pdfs_batch(
    items: List[str],
    output_dir: str,
    retries: int = 2,
    concurrency: int = 2,
    overwrite: bool = False,
) -> Dict[str, Any]:
    logging.info(
        f"Batch download of {len(items)} item(s) into {output_dir} "
        f"(retries={retries}, concurrency={concurrency}, overwrite={overwrite})"
    )
    """
    Download several papers at once, with per-item retries and a JSON manifest.

    Each item may be a DOI (e.g. "10.1038/171737a0"), a direct PDF URL, a doi.org
    URL, or a Sci-Hub mirror page URL. Filenames are derived from the DOI/URL and
    sanitized for the filesystem; name collisions get numeric suffixes.

    Args:
        items (List[str]): DOIs or URLs to download. Duplicates are removed.
        output_dir (str): Directory where the PDFs (and the manifest) are written.
                Created if missing.
        retries (int, optional): Extra attempts per item after the first one for
                transient failures. Defaults to 2 (3 attempts total).
        concurrency (int, optional): Number of parallel downloads, clamped to 1-4.
                Defaults to 2.
        overwrite (bool, optional): When False (default), existing valid PDFs with
                the same derived filename are skipped.

    Returns:
        Dict[str, Any]: Summary with keys:
            - output_dir: Absolute output directory
            - manifest: Path to the timestamped JSON manifest (or None on failure)
            - total: Number of processed items
            - success / skipped / failed: Counts per status
            - results: List of per-item records with input, status, path, size,
              attempts, pdf_url, title and error
    """
    try:
        return await asyncio.to_thread(
            download_papers_batch,
            items,
            output_dir,
            retries=retries,
            concurrency=concurrency,
            overwrite=overwrite,
        )
    except Exception as e:
        return {"error": f"An error occurred while running the batch download: {str(e)}"}

@mcp.tool()
async def get_paper_metadata(doi: str) -> Dict[str, Any]:
    logging.info(f"Getting metadata for paper with DOI: {doi}")
    """
    Get metadata information for a paper using its DOI.

    Args:
        doi (str): The Digital Object Identifier of the paper, a unique alphanumeric string
             that identifies the academic paper (e.g., "10.1038/nature09492").

    Returns:
        Dict[str, Any]: Dictionary containing paper metadata including:
            - doi: The DOI of the paper
            - title: The title of the paper
            - author: The author(s) of the paper
            - year: Publication year
            - pdf_url: URL to download the PDF if available
            - status: Success or error status
            - error: Error message if retrieval failed
    """
    try:
        # First search for the paper by DOI
        paper_info = await asyncio.to_thread(search_paper_by_doi, doi)
        
        if paper_info.get('status') == 'success':
            # Extract and return metadata
            return {
                'doi': doi,
                'title': paper_info.get('title', ''),
                'author': paper_info.get('author', ''),
                'year': paper_info.get('year', ''),
                'pdf_url': paper_info.get('pdf_url', ''),
                'status': 'success'
            }
        else:
            return {"error": f"Could not find metadata for paper with DOI {doi}"}
    except Exception as e:
        return {"error": f"An error occurred while getting metadata: {str(e)}"}

if __name__ == "__main__":
    logging.info("Starting Sci-Hub MCP Server")
    # Initialize and run the server
    mcp.run(transport='stdio')
