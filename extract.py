import requests
import json
import argparse
import sys
import re
import time
import random
import logging
from typing import Optional, Dict, List
from urllib.parse import urlparse
import os
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class ReviewData:
    """Data class for review information."""
    user: Optional[str]
    published_at: Optional[str]
    source: Optional[str]
    rating: Optional[int]
    content: Optional[str]


class GoogleMapsReviewScraper:
    """Professional Google Maps review scraper with rate limiting and error handling."""
    
    def __init__(self, delay_range=(1, 3), max_retries=3, timeout=10):
        """
        Initialize the scraper with configuration options.
        
        Args:
            delay_range: Tuple of min and max delay between requests (seconds)
            max_retries: Maximum number of retry attempts for failed requests
            timeout: Request timeout in seconds
        """
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = self._create_session()
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy and realistic headers."""
        session = requests.Session()
        
        # Retry strategy (compatible with both old and new urllib3 versions)
        retry_kwargs = {
            'total': self.max_retries,
            'status_forcelist': [429, 500, 502, 503, 504],
            'backoff_factor': 1
        }
        
        # Handle compatibility between urllib3 versions
        try:
            retry_strategy = Retry(allowed_methods=["HEAD", "GET", "OPTIONS"], **retry_kwargs)
        except TypeError:
            # Fallback for older urllib3 versions
            retry_strategy = Retry(method_whitelist=["HEAD", "GET", "OPTIONS"], **retry_kwargs)
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # More realistic headers
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        
        return session
    
    def _validate_url(self, url: str) -> bool:
        """Validate if the URL is a proper Google Maps URL."""
        try:
            parsed = urlparse(url)
            if parsed.netloc not in ['maps.google.com', 'www.google.com']:
                self.logger.error(f"Invalid domain: {parsed.netloc}")
                return False
            return True
        except Exception as e:
            self.logger.error(f"URL validation error: {e}")
            return False
    
    def _respect_rate_limit(self):
        """Implement random delay to respect rate limits."""
        delay = random.uniform(*self.delay_range)
        self.logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def _extract_business_id(self, place_url: str) -> Optional[str]:
        """Extract business ID from Google Maps URL."""
        try:
            # First pattern
            match = re.search(r'1s(0x[a-f0-9]+:0x[a-f0-9]+)', place_url)
            if match:
                return match.group(1)
            
            # Fallback pattern
            matches = re.findall(r'(0x[a-f0-9]+:0x[a-f0-9]+)', place_url)
            if matches:
                return matches[0]
            
            self.logger.error("Could not extract business identifier from URL")
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting business ID: {e}")
            return None
    
    def _generate_reviews_url(self, place_url: str, pagination_token: str = "", tripadvisor_filter: bool = False) -> Optional[str]:
        """Generate the reviews API URL with optional TripAdvisor filter."""
        if not self._validate_url(place_url):
            return None
            
        business_id = self._extract_business_id(place_url)
        if not business_id:
            return None
        
        if tripadvisor_filter:
            # Use the TripAdvisor filter you discovered
            pb_param = (f"!1m7!1s{business_id}!6m4!4m1!1e1!4m1!1e3!13i100532569!"
                       f"2m2!1i10!2s{pagination_token}!5m2!1sxkx7aMKiOLKshbIPoZm1sAk!7e81!"
                       f"8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1")
            self.logger.debug("Using TripAdvisor server-side filter")
        else:
            # Original format without filter
            pb_param = (f"!1m6!1s{business_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s{pagination_token}!"
                       f"5m2!1sxkx7aMKiOLKshbIPoZm1sAk!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1")
        
        return f"https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=us&pb={pb_param}"
    
    def _clean_response_text(self, response_text: str) -> str:
        """Clean the response text by removing the security prefix."""
        prefixes = [")]}'\n", ")]}':"]
        for prefix in prefixes:
            if response_text.startswith(prefix):
                return response_text[len(prefix):]
        return response_text
    
    def _scrape_reviews_batch(self, reviews_url: str) -> Optional[List]:
        """Scrape a single batch of reviews."""
        self._respect_rate_limit()
        
        try:
            response = self.session.get(reviews_url, timeout=self.timeout)
            response.raise_for_status()
            
            cleaned_text = self._clean_response_text(response.text)
            
            try:
                data = json.loads(cleaned_text)
                self.logger.debug(f"Successfully parsed response with {len(data[2]) if data and len(data) > 2 and data[2] else 0} reviews")
                return data
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON response: {e}")
                self._save_debug_response(response.text)
                return None
                
        except requests.exceptions.Timeout:
            self.logger.warning("Request timeout - server may be slow")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error during request: {e}")
            return None
    
    def _save_debug_response(self, response_text: str):
        """Save raw response for debugging purposes."""
        debug_file = f"debug_response_{int(time.time())}.txt"
        try:
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response_text)
            self.logger.info(f"Debug response saved to {debug_file}")
        except Exception as e:
            self.logger.error(f"Failed to save debug response: {e}")
    
    def _extract_review_data(self, review: List) -> ReviewData:
        """Extract review data from the raw response structure."""
        try:
            return ReviewData(
                user=self._safe_extract(review, [0, 1, 4, 5, 0]),
                published_at=self._safe_extract(review, [0, 1, 6]),
                source=self._safe_extract(review, [0, 1, 13, -2]),
                rating=self._safe_extract(review, [0, 1, 13, -1]),
                content=self._safe_extract(review, [0, 2, 15, 0, 0])
            )
        except Exception as e:
            self.logger.warning(f"Skipping malformed review: {e}")
            return ReviewData(None, None, None, None, None)
    
    def _safe_extract(self, data, path):
        """Safely extract nested data using a path list."""
        try:
            current = data
            for key in path:
                if isinstance(current, list) and len(current) > abs(key):
                    current = current[key]
                elif isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current
        except (IndexError, TypeError, KeyError):
            return None
    
    def scrape_all_reviews(self, place_url: str, tripadvisor_only: bool = False, max_pages: int = None) -> List[Dict]:
        """
        Scrape all reviews from a Google Maps location.
        
        Args:
            place_url: Google Maps URL of the business
            tripadvisor_only: If True, only extract TripAdvisor reviews using server-side filtering
            max_pages: Maximum number of pages to scrape (None for unlimited)
        
        Returns:
            List of review dictionaries
        """
        self.logger.info(f"Starting review scraping for URL: {place_url}")
        if tripadvisor_only:
            self.logger.info("Using server-side TripAdvisor filter")
        
        all_reviews = []
        pagination_token = ""
        page_count = 0
        
        while True:
            if max_pages and page_count >= max_pages:
                self.logger.info(f"Reached maximum page limit: {max_pages}")
                break
                
            reviews_url = self._generate_reviews_url(place_url, pagination_token, tripadvisor_only)
            if not reviews_url:
                break
            
            data = self._scrape_reviews_batch(reviews_url)
            if not data or len(data) < 3 or not data[2]:
                self.logger.info("No more reviews found or invalid response")
                break
            
            page_reviews = 0            
            for raw_review in data[2]:
                review_data = self._extract_review_data(raw_review)
                
                # When using server-side filter, we shouldn't need client-side filtering
                # but keep it as a safety check
                if tripadvisor_only and review_data.source and review_data.source.lower() != "tripadvisor":
                    self.logger.debug(f"Unexpected non-TripAdvisor review found: {review_data.source}")
                    continue
                
                # Convert to dictionary for JSON serialization
                review_dict = {
                    "user": review_data.user,
                    "published_at": review_data.published_at,
                    "source": review_data.source,
                    "rating": review_data.rating,
                    "content": review_data.content
                }
                
                all_reviews.append(review_dict)
                page_reviews += 1
            
            self.logger.info(f"Page {page_count + 1}: Collected {page_reviews} reviews")
            
            # Check for next page
            pagination_token = data[1] if len(data) > 1 else None
            if not pagination_token:
                self.logger.info("No more pages available")
                break
            
            page_count += 1
        
        self.logger.info(f"Scraping completed. Total reviews collected: {len(all_reviews)}")
        if tripadvisor_only and len(all_reviews) == 0:
            self.logger.warning("No TripAdvisor reviews found for this location. The business might not have TripAdvisor integration.")
        
        return all_reviews
    
    def save_reviews(self, reviews: List[Dict], filename: str = "reviews.json"):
        """Save reviews to a JSON file."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(reviews, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"✅ Saved {len(reviews)} reviews to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save reviews: {e}")
            raise


def main():
    """Main function to handle command line arguments and run the scraper."""
    parser = argparse.ArgumentParser(
        description="Professional Google Maps review scraper with rate limiting and error handling."
    )
    parser.add_argument('url', help="The Google Maps URL of the business")
    parser.add_argument('--tripadvisor', action='store_true', 
                       help="Extract only TripAdvisor reviews")
    parser.add_argument('--output', '-o', default="reviews.json", 
                       help="Output filename (default: reviews.json)")
    parser.add_argument('--delay', type=float, nargs=2, default=[1, 3], 
                       metavar=('MIN', 'MAX'),
                       help="Delay range between requests in seconds (default: 1 3)")
    parser.add_argument('--max-pages', type=int, 
                       help="Maximum number of pages to scrape")
    parser.add_argument('--timeout', type=int, default=10, 
                       help="Request timeout in seconds (default: 10)")
    parser.add_argument('--max-retries', type=int, default=3, 
                       help="Maximum retry attempts (default: 3)")
    
    args = parser.parse_args()
    
    try:
        # Create scraper instance
        scraper = GoogleMapsReviewScraper(
            delay_range=tuple(args.delay),
            max_retries=args.max_retries,
            timeout=args.timeout
        )
        
        # Scrape reviews
        reviews = scraper.scrape_all_reviews(
            place_url=args.url,
            tripadvisor_only=args.tripadvisor,
            max_pages=args.max_pages
        )
        
        # Save results
        scraper.save_reviews(reviews, args.output)
        
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()