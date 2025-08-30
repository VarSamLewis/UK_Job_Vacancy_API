import os
import time
import requests
from bs4 import BeautifulSoup
import urllib.parse
from pathlib import Path
import re
from typing import List, Set, Dict, Optional, Tuple
from dataclasses import dataclass
import random

@dataclass
class DownloadResult:
    """Result of a download attempt"""
    success: bool
    filename: str
    file_size: int = 0
    error_message: str = ""
    url: str = ""

@dataclass
class DatasetResult:
    """Result of processing a single dataset URL"""
    url: str
    dataset_name: str
    files_found: int
    files_downloaded: int
    downloaded_files: List[DownloadResult]
    errors: List[str]

class ONSExcelDownloader:
    """
    A class to download Excel files from ONS (Office for National Statistics) websites
    using Beautiful Soup for HTML parsing and requests for downloading.
    Enhanced with better rate limiting and retry mechanisms.
    """
    
    def __init__(self, 
                 download_path: str,
                 user_agent: str = None,
                 timeout: int = 30,
                 delay_between_files: int = 5,
                 delay_between_urls: int = 10,
                 max_retries: int = 3,
                 retry_delay: int = 30):
        """
        Initialize the ONS Excel Downloader
        
        Args:
            download_path: Directory to save downloaded files
            user_agent: Custom user agent string
            timeout: Request timeout in seconds
            delay_between_files: Delay between file downloads in seconds
            delay_between_urls: Delay between processing URLs in seconds
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Base delay for retries in seconds
        """
        self.download_path = Path(download_path)
        self.timeout = timeout
        self.delay_between_files = delay_between_files
        self.delay_between_urls = delay_between_urls
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.base_url = "https://www.ons.gov.uk"
        
        # Setup headers with more realistic browser simulation
        self.headers = {
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Create download directory
        self.download_path.mkdir(parents=True, exist_ok=True)
        
        # Statistics
        self.stats = {
            'urls_processed': 0,
            'files_found': 0,
            'files_downloaded': 0,
            'total_size': 0,
            'errors': []
        }
        
        # Session for connection reuse
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def wait_with_jitter(self, base_delay: int, multiplier: float = 1.0):
        """Add random jitter to delays to avoid synchronized requests"""
        jitter = random.uniform(0.5, 1.5)
        actual_delay = base_delay * multiplier * jitter
        time.sleep(actual_delay)
    
    def make_request_with_retry(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """
        Make HTTP request with exponential backoff retry for rate limiting
        
        Args:
            url: URL to request
            method: HTTP method
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.max_retries + 1):
            try:
                if method.upper() == 'HEAD':
                    response = self.session.head(url, timeout=self.timeout, **kwargs)
                else:
                    response = self.session.get(url, timeout=self.timeout, **kwargs)
                
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        retry_wait = self.retry_delay * (2 ** attempt)
                        jitter = random.uniform(0.8, 1.2)
                        actual_wait = retry_wait * jitter
                        
                        print(f"   Rate limited (429). Waiting {actual_wait:.1f}s before retry {attempt + 1}/{self.max_retries}...")
                        time.sleep(actual_wait)
                        continue
                    else:
                        print(f"   Rate limited after {self.max_retries} retries. Skipping.")
                        return None
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** attempt) * random.uniform(0.8, 1.2)
                    print(f"   Request failed: {str(e)}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"   Request failed after {self.max_retries} retries: {str(e)}")
                    return None
        
        return None
    
    def extract_dataset_name(self, url: str) -> str:
        """Extract dataset name from URL for file naming"""
        try:
            match = re.search(r'/datasets/([^/]+)', url)
            if match:
                dataset_part = match.group(1)
                # Extract alphanumeric identifier (like x06, vacs01, etc.)
                identifier_match = re.search(r'^([a-zA-Z]+\d+)', dataset_part)
                if identifier_match:
                    return identifier_match.group(1).upper()
                # Fallback to first part before special chars
                return dataset_part.split('_')[0].split('-')[0].upper()[:20]
            return "ons_dataset"
        except Exception:
            return "ons_dataset"
    
    def validate_excel_file(self, response: requests.Response) -> Tuple[bool, str]:
        """
        Validate that the response contains an Excel file
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        content_type = response.headers.get('Content-Type', '').lower()
        
        # Check for HTML content (error pages)
        if 'html' in content_type or 'text' in content_type:
            return False, f"Response is HTML/text instead of Excel file (Content-Type: {content_type})"
        
        # Check for empty content
        content_length = int(response.headers.get('Content-Length', 0))
        if content_length > 0 and content_length < 1000:
            return False, f"File too small ({content_length} bytes) - likely an error page"
        
        return True, ""
    
    def download_file(self, url: str, filename: str, verbose: bool = True) -> DownloadResult:
        """
        Download a single Excel file with validation and retry logic
        
        Args:
            url: URL of the file to download
            filename: Name to save the file as
            verbose: Whether to print progress information
            
        Returns:
            DownloadResult object with success status and details
        """
        file_path = self.download_path / filename
        
        try:
            if verbose:
                print(f"   Requesting: {url}")
            
            # Setup headers for Excel files
            download_headers = {
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Referer': 'https://www.ons.gov.uk/',
                'Accept-Encoding': 'gzip, deflate, br'
            }
            
            # HEAD request to validate with retry
            head_response = self.make_request_with_retry(url, 'HEAD', headers=download_headers, allow_redirects=True)
            if not head_response:
                return DownloadResult(False, filename, 0, "Failed to get file headers after retries", url)
            
            if verbose:
                print(f"   Status: {head_response.status_code}")
                print(f"   Content-Type: {head_response.headers.get('Content-Type', 'Unknown')}")
                print(f"   Content-Length: {head_response.headers.get('Content-Length', 'Unknown')}")
            
            # Validate response
            is_valid, error_msg = self.validate_excel_file(head_response)
            if not is_valid:
                return DownloadResult(False, filename, 0, error_msg, url)
            
            # Small delay before actual download
            self.wait_with_jitter(2)
            
            # Download the file with retry
            response = self.make_request_with_retry(url, 'GET', stream=True, headers=download_headers, allow_redirects=True)
            if not response:
                return DownloadResult(False, filename, 0, "Failed to download file after retries", url)
            
            # Final validation
            is_valid, error_msg = self.validate_excel_file(response)
            if not is_valid:
                return DownloadResult(False, filename, 0, error_msg, url)
            
            # Save file with progress
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if verbose and total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            print(f"   Progress: {progress:.1f}%", end='\r')
            
            if verbose:
                print()  # New line after progress
            
            # Verify downloaded file
            if file_path.exists():
                file_size = file_path.stat().st_size
                if file_size < 1000:
                    # Read first few bytes for debugging
                    with open(file_path, 'rb') as f:
                        first_bytes = f.read(200)
                    
                    file_path.unlink()  # Remove corrupted file
                    return DownloadResult(False, filename, 0, 
                                        f"Downloaded file too small ({file_size} bytes). First bytes: {first_bytes[:50]}...", url)
                
                if verbose:
                    print(f"   Downloaded: {filename} ({file_size / 1024:.1f} KB)")
                
                self.stats['total_size'] += file_size
                return DownloadResult(True, filename, file_size, "", url)
            
            return DownloadResult(False, filename, 0, "File not created after download", url)
            
        except Exception as e:
            return DownloadResult(False, filename, 0, f"Download failed: {str(e)}", url)
    
    def extract_excel_links(self, html_content: str, base_url: str = None) -> Set[str]:
        """
        Extract all Excel file links from HTML using Beautiful Soup
        
        Args:
            html_content: HTML content to parse
            base_url: Base URL for resolving relative links
            
        Returns:
            Set of absolute URLs to Excel files
        """
        if base_url is None:
            base_url = self.base_url
            
        soup = BeautifulSoup(html_content, 'html.parser')
        excel_links = set()
        excel_extensions = ['.xlsx', '.xls']
        
        # Strategy 1: Find all links with Excel extensions
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link['href']
            if any(ext.lower() in href.lower() for ext in excel_extensions):
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    full_url = base_url + href
                elif not href.startswith('http'):
                    full_url = f"{base_url}/{href.lstrip('/')}"
                else:
                    full_url = href
                
                excel_links.add(full_url)
        
        # Strategy 2: Look in tables (ONS often uses tables for file listings)
        tables = soup.find_all('table')
        for table in tables:
            table_links = table.find_all('a', href=True)
            for link in table_links:
                href = link['href']
                if any(ext.lower() in href.lower() for ext in excel_extensions):
                    if href.startswith('/'):
                        full_url = base_url + href
                    elif not href.startswith('http'):
                        full_url = f"{base_url}/{href.lstrip('/')}"
                    else:
                        full_url = href
                    excel_links.add(full_url)
        
        # Strategy 3: Look for download-specific elements
        download_elements = soup.find_all(['div', 'section'], 
                                        class_=lambda x: x and any(keyword in x.lower() 
                                        for keyword in ['download', 'file', 'superseded']))
        
        for element in download_elements:
            element_links = element.find_all('a', href=True)
            for link in element_links:
                href = link['href']
                if any(ext.lower() in href.lower() for ext in excel_extensions):
                    if href.startswith('/'):
                        full_url = base_url + href
                    elif not href.startswith("http"):
                        full_url = f"{base_url}/{href.lstrip('/')}"
                    else:
                        full_url = href
                    excel_links.add(full_url)
        
        return excel_links
    
    def get_filename_from_url(self, url: str, dataset_name: str, index: int) -> str:
        """
        Extract or generate appropriate filename from URL
        
        Args:
            url: URL of the file
            dataset_name: Name of the dataset
            index: Index for fallback naming
            
        Returns:
            Appropriate filename for the file
        """
        # Try to get filename from URL path
        parsed_url = urllib.parse.urlparse(url)
        filename = os.path.basename(parsed_url.path)
        
        # If we have a good filename, use it
        if filename and filename.lower().endswith(('.xlsx', '.xls')):
            return filename
        
        # Try to get filename from Content-Disposition header
        try:
            head_response = self.make_request_with_retry(url, 'HEAD', allow_redirects=True)
            if head_response:
                content_disposition = head_response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disposition:
                    filename = content_disposition.split('filename=')[1].strip('"\'')
                    if filename and filename.lower().endswith(('.xlsx', '.xls')):
                        return filename
        except Exception:
            pass
        
        # Fallback to generated filename
        return f"{dataset_name.lower()}_file_{index}.xlsx"
    
    def ensure_unique_filename(self, filename: str) -> str:
        """
        Ensure filename is unique in the download directory
        
        Args:
            filename: Desired filename
            
        Returns:
            Unique filename
        """
        file_path = self.download_path / filename
        if not file_path.exists():
            return filename
        
        base_name, ext = os.path.splitext(filename)
        counter = 1
        
        while True:
            new_filename = f"{base_name}_{counter}{ext}"
            new_path = self.download_path / new_filename
            if not new_path.exists():
                return new_filename
            counter += 1
    
    def process_url(self, url: str, verbose: bool = True) -> DatasetResult:
        """
        Process a single URL to find and download all Excel file links on the page.
        Enhanced with better error handling and rate limiting.
            
        Args:
            url: URL to process
            verbose: Whether to print detailed progress

        Returns:
            DatasetResult with processing details
        """
        dataset_name = self.extract_dataset_name(url)
        result = DatasetResult(
            url=url,
            dataset_name=dataset_name,
            files_found=0,
            files_downloaded=0,
            downloaded_files=[],
            errors=[]
        )

        try:
            if verbose:
                print(f"Fetching webpage: {url}")

            # Fetch the webpage with retry mechanism
            page_headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }

            response = self.make_request_with_retry(url, headers=page_headers)
            if not response:
                error_msg = "Failed to fetch webpage after retries"
                result.errors.append(error_msg)
                return result

            if verbose:
                print(f"Status: {response.status_code}")
                print(f"Page size: {len(response.content)} bytes")

            # Find all Excel links on the page
            soup = BeautifulSoup(response.text, 'html.parser')
            excel_extensions = ('.xlsx', '.xls')
            excel_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if any(href.lower().endswith(ext) for ext in excel_extensions):
                    # Convert relative URLs to absolute
                    if href.startswith('/'):
                        full_url = self.base_url + href
                    elif not href.startswith('http'):
                        full_url = f"{self.base_url}/{href.lstrip('/')}"
                    else:
                        full_url = href
                    excel_links.append(full_url)

            result.files_found = len(excel_links)
            if not excel_links:
                error_msg = "No Excel files found on this page"
                result.errors.append(error_msg)
                if verbose:
                    print(f"Found 0 Excel file(s) on the page")
                    print(f"Dataset: {dataset_name}")
                return result

            if verbose:
                print(f"Found {len(excel_links)} Excel file(s) on the page")
                print(f"Dataset: {dataset_name}")

            # Download all Excel files found
            if verbose:
                print("Files to download (all Excel links):")
            for i, link in enumerate(excel_links, 1):
                filename = self.get_filename_from_url(link, dataset_name, i)
                filename = self.ensure_unique_filename(filename)
                if verbose:
                    print(f"  {i}. {filename} -> {link}")

            # Actually download the files
            for i, link in enumerate(excel_links, 1):
                filename = self.get_filename_from_url(link, dataset_name, i)
                filename = self.ensure_unique_filename(filename)
                
                if verbose:
                    print(f"\nDownloading file {i}/{len(excel_links)}: {filename}")
                
                download_result = self.download_file(link, filename, verbose)
                result.downloaded_files.append(download_result)
                
                if download_result.success:
                    result.files_downloaded += 1
                    self.stats['files_downloaded'] += 1
                else:
                    result.errors.append(f"Failed to download {filename}: {download_result.error_message}")
                    if verbose:
                        print(f"Error: {download_result.error_message}")
                
                # Wait between files with jitter
                if i < len(excel_links) and self.delay_between_files > 0:
                    if verbose:
                        print(f"   Waiting {self.delay_between_files}s before next file...")
                    self.wait_with_jitter(self.delay_between_files)

            self.stats['files_found'] += result.files_found

        except Exception as e:
            error_msg = f"Error processing URL {url}: {str(e)}"
            result.errors.append(error_msg)
            self.stats['errors'].append(error_msg)
            if verbose:
                print(f"Error: {error_msg}")

        return result

    def download_from_urls(self, urls: List[str], verbose: bool = True) -> List[DatasetResult]:
        """
        Download Excel files from multiple URLs with improved rate limiting
        
        Args:
            urls: List of URLs to process
            verbose: Whether to print detailed progress

        Returns:
            List of DatasetResult objects
        """
        results = []

        if verbose:
            print(f"Processing {len(urls)} URL(s)...")
            print(f"Download directory: {self.download_path}")
            print(f"Rate limiting: {self.delay_between_urls}s between URLs, {self.delay_between_files}s between files")

        for i, url in enumerate(urls, 1):
            if verbose:
                print(f"\n{'='*60}")
                print(f"Processing URL {i}/{len(urls)}: {self.extract_dataset_name(url)}")
                print(f"   {url}")

            result = self.process_url(url, verbose)
            results.append(result)
            self.stats['urls_processed'] += 1

            # Delay between URLs with jitter
            if i < len(urls) and self.delay_between_urls > 0:
                if verbose:
                    print(f"Waiting {self.delay_between_urls}s before next URL...")
                self.wait_with_jitter(self.delay_between_urls)

        return results

    def print_summary(self, results: List[DatasetResult]):
        """Print a summary of the download session"""
        print(f"\n{'='*60}")
        print(f"Download Session Complete!")
        print(f"{'='*60}")

        print(f"Summary:")
        print(f"   URLs processed: {self.stats['urls_processed']}")
        print(f"   Files found: {self.stats['files_found']}")
        print(f"   Files downloaded: {self.stats['files_downloaded']}")
        print(f"   Total size: {self.stats['total_size'] / 1024 / 1024:.1f} MB")
        print(f"   Errors: {len(self.stats['errors'])}")

        if results:
            print(f"\nPer Dataset Results:")
            for result in results:
                success_rate = (result.files_downloaded / result.files_found * 100) if result.files_found > 0 else 0
                print(f"   [{result.dataset_name}] {result.files_downloaded}/{result.files_found} files ({success_rate:.1f}%)")

                # Show successful downloads
                successful_files = [dr for dr in result.downloaded_files if dr.success]
                if successful_files:
                    for file_result in successful_files:
                        print(f"      {file_result.filename} ({file_result.file_size / 1024:.1f} KB)")

                # Show errors
                if result.errors:
                    for error in result.errors[:3]:  # Show first 3 errors
                        print(f"      Error: {error}")
                    if len(result.errors) > 3:
                        print(f"      ... and {len(result.errors) - 3} more errors")
        
        # Suggestions for failed downloads
        failed_results = [r for r in results if r.files_downloaded < r.files_found or r.errors]
        if failed_results:
            print(f"\nTroubleshooting suggestions:")
            print(f"   - Try increasing delays (currently {self.delay_between_urls}s between URLs)")
            print(f"   - Run script during off-peak hours")
            print(f"   - Process failed URLs individually with longer delays")
    
    def get_existing_files(self) -> List[Dict]:
        """Get list of existing Excel files in download directory"""
        existing_files = []
        
        if self.download_path.exists():
            for file_path in self.download_path.glob('*.xls*'):
                if file_path.is_file():
                    stat = file_path.stat()
                    existing_files.append({
                        'name': file_path.name,
                        'size': stat.st_size,
                        'path': file_path
                    })
        
        return existing_files
    
    def download_single_url(self, url: str, verbose: bool = True) -> DatasetResult:
        """
        Download from a single URL - useful for retry attempts
        
        Args:
            url: URL to process
            verbose: Whether to print detailed progress
            
        Returns:
            DatasetResult object
        """
        if verbose:
            print(f"Processing single URL: {self.extract_dataset_name(url)}")
            print(f"URL: {url}")
        
        return self.process_url(url, verbose)
        
if __name__ == "__main__":
    print("ONS Excel Downloader Class - Enhanced Version")
    print("=" * 60)
    
    # Configuration - increased delays for better rate limiting
    urls_to_process = [
        "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesandunemploymentvacs01",
        "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbyindustryvacs02",
        "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbysizeofbusinessvacs03",
        "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/x06singlemonthvacanciesestimatesnotdesignatedasnationalstatistics"
    ]
    
    download_path = r"C:\Users\samle\Source\Repos\UK_Job_Vacancy_API\Data"
    
    # Enhanced configuration for better rate limiting
    downloader = ONSExcelDownloader(
        download_path=download_path,
        timeout=60,
        delay_between_files=8,  # Increased from 2 to 8 seconds
        delay_between_urls=15,  # Increased from 3 to 15 seconds
        max_retries=3,
        retry_delay=45  # 45 seconds base retry delay
    )
    
    # Check existing files
    existing_files = downloader.get_existing_files()
    if existing_files:
        print(f"Found {len(existing_files)} existing files:")
        total_size = sum(f['size'] for f in existing_files)
        for file_info in existing_files[:5]:  # Show first 5
            print(f"   - {file_info['name']} ({file_info['size'] / 1024:.1f} KB)")
        if len(existing_files) > 5:
            print(f"   ... and {len(existing_files) - 5} more files")
        print(f"Total existing size: {total_size / 1024 / 1024:.1f} MB")
        
        print("\nOptions:")
        print("1. Continue with all URLs (recommended with longer delays)")
        print("2. Process failed URLs only")
        print("3. Cancel")
        
        choice = input("Enter choice (1-3): ").strip()
        
        if choice == "2":
            # Only process URLs that failed last time
            failed_urls = [
                "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbyindustryvacs02",
                "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbysizeofbusinessvacs03",
                "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/x06singlemonthvacanciesestimatesnotdesignatedasnationalstatistics"
            ]
            urls_to_process = failed_urls
            print(f"Processing {len(failed_urls)} failed URLs with enhanced rate limiting...")
        elif choice == "3":
            print("Download cancelled.")
            exit()
        # Choice 1 continues with all URLs
    
    # Download files
    print(f"\nStarting download with enhanced rate limiting...")
    print(f"Delays: {downloader.delay_between_urls}s between URLs, {downloader.delay_between_files}s between files")
    print(f"Retries: Up to {downloader.max_retries} attempts with exponential backoff")
    
    results = downloader.download_from_urls(urls_to_process)
    
    # Print summary
    downloader.print_summary(results)
    
    print("\nEnhanced downloader finished!")
    
    # Suggest individual retry for any remaining failures
    failed_results = [r for r in results if r.files_downloaded < r.files_found or r.errors]
    if failed_results:
        print(f"\nFor remaining failures, try processing URLs individually:")
        for failed_result in failed_results:
            print(f"   python -c \"from src.ingestion.web_scraper import ONSExcelDownloader; d = ONSExcelDownloader('{download_path}', delay_between_urls=30, retry_delay=60); d.download_single_url('{failed_result.url}')\"")