import os
import time
import requests
from bs4 import BeautifulSoup
import urllib.parse
from pathlib import Path
import re
from typing import List, Set, Dict, Optional, Tuple
from dataclasses import dataclass

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
    """
    
    def __init__(self, 
                 download_path: str,
                 user_agent: str = None,
                 timeout: int = 30,
                 delay_between_files: int = 2,
                 delay_between_urls: int = 3,
                 min_request_interval: int = 10):
        """
        Initialize the ONS Excel Downloader
        
        Args:
            download_path: Directory to save downloaded files
            user_agent: Custom user agent string
            timeout: Request timeout in seconds
            delay_between_files: Delay between file downloads in seconds
            delay_between_urls: Delay between processing URLs in seconds
        """
        self.download_path = Path(download_path)
        self.timeout = timeout
        self.delay_between_files = delay_between_files
        self.delay_between_urls = delay_between_urls
        self.base_url = "https://www.ons.gov.uk"
        self.last_request_time = 0
        self.min_request_interval = min_request_interval  # seconds
        
        # Setup headers
        self.headers = {
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
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
                return dataset_part.split('_')[0].split('-')[0].upper()[:10]
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
        Download a single Excel file with validation, enforcing a minimum delay between requests.
        """
        file_path = self.download_path / filename

        try:
            # Enforce minimum delay between requests
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_request_interval:
                sleep_time = self.min_request_interval - elapsed
                if verbose:
                    print(f"Waiting {sleep_time:.1f}s before next request to respect rate limits...")
                time.sleep(sleep_time)
            self.last_request_time = time.time()

            if verbose:
                print(f"   Requesting: {url}")

            # Setup headers for Excel files
            download_headers = {
                **self.headers,
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Referer': 'https://www.ons.gov.uk/',
                'Accept-Encoding': 'identity'
            }

            # HEAD request to validate
            head_response = requests.head(url, headers=download_headers, allow_redirects=True, timeout=self.timeout)

            if verbose:
                print(f"   Status: {head_response.status_code}")
                print(f"   Content-Type: {head_response.headers.get('Content-Type', 'Unknown')}")
                print(f"   Content-Length: {head_response.headers.get('Content-Length', 'Unknown')}")

            # Validate response
            is_valid, error_msg = self.validate_excel_file(head_response)
            if not is_valid:
                return DownloadResult(False, filename, 0, error_msg, url)

            # Enforce minimum delay before GET request
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_request_interval:
                sleep_time = self.min_request_interval - elapsed
                if verbose:
                    print(f"Waiting {sleep_time:.1f}s before next request to respect rate limits...")
                time.sleep(sleep_time)
            self.last_request_time = time.time()

            # Download the file
            response = requests.get(url, stream=True, headers=download_headers, allow_redirects=True, timeout=120)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', '60'))
                print(f"Rate limited. Waiting {retry_after} seconds before retrying...")
                time.sleep(retry_after)
                self.last_request_time = time.time()
                response = requests.get(url, stream=True, headers=download_headers, allow_redirects=True, timeout=120)
            response.raise_for_status()

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
                    with open(file_path, 'rb') as f:
                        first_bytes = f.read(200)
                    file_path.unlink()
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
                    elif not href.startswith('http'):
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
            head_response = requests.head(url, headers=self.headers, allow_redirects=True, timeout=15)
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
        Process a single URL to find and download all Excel files
        
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
            
            # Fetch the webpage
            page_headers = {
                **self.headers,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            
            response = requests.get(url, headers=page_headers, timeout=self.timeout)
            response.raise_for_status()
            
            if verbose:
                print(f"Status: {response.status_code}")
                print(f"Page size: {len(response.content)} bytes")
            
            # Extract Excel links
            excel_links = self.extract_excel_links(response.text)
            result.files_found = len(excel_links)
            
            if not excel_links:
                error_msg = "No Excel files found on this page"
                result.errors.append(error_msg)
                if verbose:
                    print(f"Error: {error_msg}")
                return result
            
            if verbose:
                print(f"Found {len(excel_links)} Excel file(s)")
                print(f"Dataset: {dataset_name}")
            
            # Download each file
            for i, link in enumerate(excel_links, 1):
                try:
                    # Generate filename
                    filename = self.get_filename_from_url(link, dataset_name, i)
                    filename = self.ensure_unique_filename(filename)
                    
                    if verbose:
                        print(f"\nDownloading file {i}/{len(excel_links)}: {filename}")
                    
                    # Download the file
                    download_result = self.download_file(link, filename, verbose)
                    result.downloaded_files.append(download_result)
                    
                    if download_result.success:
                        result.files_downloaded += 1
                        self.stats['files_downloaded'] += 1
                    else:
                        result.errors.append(f"Failed to download {filename}: {download_result.error_message}")
                        if verbose:
                            print(f"Error: {download_result.error_message}")
                    
                    # Delay between files
                    if i < len(excel_links) and self.delay_between_files > 0:
                        time.sleep(self.delay_between_files)
                        
                except Exception as e:
                    error_msg = f"Error processing file {i}: {str(e)}"
                    result.errors.append(error_msg)
                    if verbose:
                        print(f"Error: {error_msg}")
            
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
        Download Excel files from multiple URLs
        
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
        
        for i, url in enumerate(urls, 1):
            if verbose:
                print(f"\n{'='*60}")
                print(f"Processing URL {i}/{len(urls)}: {self.extract_dataset_name(url)}")
                print(f"   {url}")
            
            result = self.process_url(url, verbose)
            results.append(result)
            self.stats['urls_processed'] += 1
            
            # Delay between URLs
            if i < len(urls) and self.delay_between_urls > 0:
                if verbose:
                    print(f"Waiting {self.delay_between_urls}s before next URL...")
                time.sleep(self.delay_between_urls)
        
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

# Example usage and convenience functions
def create_downloader(download_path: str, **kwargs) -> ONSExcelDownloader:
    """Convenience function to create a downloader instance"""
    return ONSExcelDownloader(download_path, **kwargs)

def quick_download(urls: List[str], download_path: str, verbose: bool = True) -> List[DatasetResult]:
    """Quick download function for simple use cases"""
    downloader = ONSExcelDownloader(download_path)
    results = downloader.download_from_urls(urls, verbose)
    if verbose:
        downloader.print_summary(results)
    return results

# Example usage
if __name__ == "__main__":
    print("ONS Excel Downloader Class")
    print("=" * 60)
    
    # Configuration
    urls_to_process = [
        "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesandunemploymentvacs01/current"
        #,"https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbyindustryvacs02/current"
        #,"https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesbysizeofbusinessvacs03/current"
        #,"https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/x06singlemonthvacanciesestimatesnotdesignatedasnationalstatistics/current"
    ]
    
    download_path = r"C:\Users\samle\Source\Repos\UK_Job_Vacancy_API\Data"
    
    # Method 1: Using the class directly
    downloader = ONSExcelDownloader(
        download_path=download_path,
        timeout=30,
        delay_between_files=2,
        delay_between_urls=3
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
        
        response = input("\nContinue with download? (y/n): ")
        if response.lower() != 'y':
            print("Download cancelled.")
            exit()
    
    # Download files
    results = downloader.download_from_urls(urls_to_process)
    
    # Print summary
    downloader.print_summary(results)
    
    print("\nClass-based downloader finished!")

# Alternative quick usage:
"""
# Method 2: Quick download function
results = quick_download(urls_to_process, download_path)

# Method 3: Custom configuration
downloader = create_downloader(
    download_path, 
    timeout=60, 
    delay_between_files=1
)
results = downloader.download_from_urls(urls_to_process)
"""