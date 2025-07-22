# Google Maps Reviews Scraper

A lightweight Python script to fetch user reviews from Google Maps locations. Supports pagination, error handling, and optional server‑side filtering for TripAdvisor‑sourced reviews.

## Features

- Extract review data: author, rating, published date, source, and content  
- Handles pagination and retries on transient errors  
- Configurable rate limits (random delays) to avoid triggering bot protection  
- Optional server‑side filter for TripAdvisor reviews  
- Command‑line interface with customizable output filename

## Installation

1. Clone the repository:  
   ```bash
   git clone https://github.com/mookamal/google-maps-reviews-scraper.git
   cd google-maps-reviews-scraper

2. Install dependencies:  
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt


## Usage

python extract.py "https://www.google.com/maps/place/…"

python extract.py "https://www.google.com/maps/place/…" --tripadvisor

python extract.py URL \
  --delay 1 3 \
  --timeout 15 \
  --max-retries 5 \
  --output reviews.json

## Output
Results are saved as a JSON array of objects:

[
  {
    "user": "Jane Doe",
    "rating": 5,
    "published_at": "3 weeks ago",
    "source": "TripAdvisor",
    "content": "Excellent stay..."
  },
  …
]
