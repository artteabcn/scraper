"""
Universal Lead Scraper — Multi-country, Multi-industry, Multi-source
Supports: Hotels, Restaurants | Sources: Maps, Booking, TripAdvisor, Facebook, Instagram, Agoda
Features: Jitter, cross-source deduplication, phone normalization, Google Sheets export

Usage:
    python universal_scraper.py --config config.json
    python universal_scraper.py --preset thailand_hotels
    python universal_scraper.py --location "Bali" --country "Indonesia" --industry restaurant --sources maps,instagram,agoda
"""

import asyncio
import csv
import json
import os
import re
import sys
import argparse
import random
import time
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus, urlparse
from functools import lru_cache

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Booking engine detection signals
DIRECT_BOOKING_SIGNALS = [
    "check availability", "book now", "book direct", "book a room",
    "reserve now", "reserve a room", "online reservation", "book online",
    "check-in date", "check-out date", "arrival date", "departure date",
    "select dates", "number of nights", "room availability",
    "cloudbeds", "lodgify", "beds24", "siteminder", "rezovation",
    "webrezpro", "clockwork", "hotelez", "myallocator", "djubo",
    "rms.com", "little hotelier", "guestline", "apaleo",
    "widget.booking.com/static/dist", "be.synxis.com",
    "bookassist", "triptease", "directbooking",
    # Restaurant-specific booking
    "resy.com", "opentable.com", "tockify", "sevenrooms", "quandoo",
    "reservation", "table booking", "book a table", "reserve table",
]

OTA_REDIRECT_SIGNALS = [
    "booking.com", "agoda.com", "expedia.com", "airbnb.com",
    "hotels.com", "tripadvisor.com", "hostelworld.com",
    # Restaurant OTAs
    "ubereats.com", "deliveroo", "grubhub", "doordash",
]

FB_RE = re.compile(r"facebook\.com/([\w.\-]+)", re.IGNORECASE)
IG_RE = re.compile(r"instagram\.com/([\w.]+)", re.IGNORECASE)

# ────────────────────────────────────────────────────────────
# PHONE NORMALIZATION
# ────────────────────────────────────────────────────────────

COUNTRY_CODES = {
    "thailand": "66", "th": "66",
    "indonesia": "62", "id": "62",
    "vietnam": "84", "vn": "84",
    "philippines": "63", "ph": "63",
    "malaysia": "60", "my": "60",
    "singapore": "65", "sg": "65",
    "cambodia": "855", "kh": "855",
    "laos": "856", "la": "856",
    "myanmar": "95", "mm": "95",
    "india": "91", "in": "91",
    "japan": "81", "jp": "81",
    "south korea": "82", "kr": "82",
    "china": "86", "cn": "86",
    "australia": "61", "au": "61",
    "united states": "1", "us": "1", "usa": "1",
    "united kingdom": "44", "uk": "44", "gb": "44",
    "france": "33", "fr": "33",
    "germany": "49", "de": "49",
    "spain": "34", "es": "34",
    "italy": "39", "it": "39",
    "portugal": "351", "pt": "351",
    "netherlands": "31", "nl": "31",
    "belgium": "32", "be": "32",
    "switzerland": "41", "ch": "41",
    "austria": "43", "at": "43",
    "greece": "30", "gr": "30",
    "turkey": "90", "tr": "90",
    "mexico": "52", "mx": "52",
    "brazil": "55", "br": "55",
    "argentina": "54", "ar": "54",
    "colombia": "57", "co": "57",
    "south africa": "27", "za": "27",
    "morocco": "212", "ma": "212",
    "egypt": "20", "eg": "20",
    "uae": "971", "united arab emirates": "971",
    "saudi arabia": "966", "sa": "966",
    "israel": "972", "il": "972",
}


def normalize_phone(phone: str, country: str = "") -> str:
    """Normalize phone to E.164 format."""
    if not phone:
        return ""
    # Remove all non-digits except leading +
    cleaned = re.sub(r"[^\d+]", "", phone.strip())
    if not cleaned:
        return ""

    # If already has country code with +
    if cleaned.startswith("+"):
        return cleaned

    # Determine country code
    cc = ""
    country_lower = country.lower().strip() if country else ""
    if country_lower in COUNTRY_CODES:
        cc = COUNTRY_CODES[country_lower]

    # Handle local formats
    if cleaned.startswith("00"):
        return "+" + cleaned[2:]

    # Thai-specific: remove leading 0, add +66
    if cc == "66" and cleaned.startswith("0"):
        return "+66" + cleaned[1:]
    if cc == "66" and not cleaned.startswith("66"):
        return "+66" + cleaned

    # Indonesia: remove leading 0, add +62
    if cc == "62" and cleaned.startswith("0"):
        return "+62" + cleaned[1:]
    if cc == "62" and not cleaned.startswith("62"):
        return "+62" + cleaned

    # Generic: if starts with country code already
    if cc and cleaned.startswith(cc):
        return "+" + cleaned

    # Generic: remove leading 0, add country code
    if cc and cleaned.startswith("0"):
        return "+" + cc + cleaned[1:]

    # Fallback: if we have a country code, prepend it
    if cc:
        return "+" + cc + cleaned

    # Last resort: return as-is with +
    return "+" + cleaned if not cleaned.startswith("+") else cleaned


# ────────────────────────────────────────────────────────────
# JITTER / RATE LIMITING
# ────────────────────────────────────────────────────────────

async def jittered_sleep(base: float = 1.0, variance: float = 0.5):
    """Sleep with random jitter to avoid pattern detection."""
    delay = base + random.uniform(-variance, variance)
    delay = max(0.3, delay)  # minimum 300ms
    await asyncio.sleep(delay)


class RateLimiter:
    """Token bucket rate limiter for requests per minute."""
    def __init__(self, requests_per_minute: int = 30):
        self.interval = 60.0 / requests_per_minute
        self.last_request = 0

    async def acquire(self):
        now = time.time()
        elapsed = now - self.last_request
        if elapsed < self.interval:
            await asyncio.sleep(self.interval - elapsed)
        self.last_request = time.time()


# ────────────────────────────────────────────────────────────
# CROSS-SOURCE DEDUPLICATION
# ────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normalize business name for deduplication."""
    n = name.lower().strip()
    # Remove common suffixes
    for suffix in [
        "hotel", "resort", "guesthouse", "bungalow", "villa",
        "bnb", "b&b", "bed and breakfast", "homestay",
        "restaurant", "cafe", "bistro", "bar", "eatery",
        "thailand", "samui", "phuket", "bali", "koh",
    ]:
        n = re.sub(rf"\b{suffix}\b", "", n)
    # Remove non-alphanumeric
    n = re.sub(r"[^a-z0-9]", "", n)
    return n


def name_similarity(a: str, b: str) -> float:
    """Simple character-based similarity for fuzzy matching."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    # Jaccard similarity on character bigrams
    set_a = set(na[i:i+2] for i in range(len(na)-1))
    set_b = set(nb[i:i+2] for i in range(len(nb)-1))
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


class Deduplicator:
    """Cross-source deduplicator with fuzzy name matching."""
    def __init__(self, name_threshold: float = 0.75):
        self.seen_names: set[str] = set()  # exact normalized names
        self.records: list[dict] = []      # for fuzzy matching
        self.threshold = name_threshold

    def is_duplicate(self, name: str, address: str = "", phone: str = "") -> bool:
        norm = normalize_name(name)
        if norm in self.seen_names:
            return True

        # Fuzzy match against existing records
        for rec in self.records:
            sim = name_similarity(name, rec["name"])
            if sim >= self.threshold:
                # Strong name match — check address/phone as confirmation
                if address and rec.get("address"):
                    addr_sim = name_similarity(address, rec["address"])
                    if addr_sim > 0.3 or phone == rec.get("phone"):
                        return True
                elif phone and phone == rec.get("phone"):
                    return True
                elif sim >= 0.9:  # Very high name similarity alone is enough
                    return True

        return False

    def add(self, name: str, address: str = "", phone: str = ""):
        self.seen_names.add(normalize_name(name))
        self.records.append({"name": name, "address": address, "phone": phone})


# ────────────────────────────────────────────────────────────
# DATA CLASSES
# ────────────────────────────────────────────────────────────

@dataclass
class Lead:
    name: str
    address: str = ""
    phone: str = ""
    phone_normalized: str = ""
    rating: str = ""
    reviews: str = ""
    website: str = ""
    has_website: str = "no"
    has_direct_booking: str = "no"
    notes: str = ""
    source: str = ""
    source_url: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    location_query: str = ""
    industry: str = "hotel"
    country: str = ""
    dedup_key: str = ""  # hash for tracking duplicates

    def to_dict(self) -> dict:
        return asdict(self)

    def compute_dedup_key(self):
        """Generate a stable deduplication key."""
        key_data = f"{normalize_name(self.name)}|{self.address.lower().strip()}|{self.phone_normalized}"
        self.dedup_key = hashlib.md5(key_data.encode()).hexdigest()[:16]


@dataclass
class ScraperConfig:
    location: str = "Koh Samui"
    country: str = "Thailand"
    province: str = "Surat Thani"
    industry: str = "hotel"  # "hotel" or "restaurant"
    sources: list[str] = field(default_factory=lambda: ["maps", "booking", "tripadvisor", "facebook", "instagram", "agoda"])
    maps_queries: list[str] = field(default_factory=list)
    booking_pages: int = 5
    tripadvisor_pages: int = 4
    agoda_pages: int = 5
    facebook_queries: list[str] = field(default_factory=list)
    instagram_queries: list[str] = field(default_factory=list)
    max_results_per_source: int = 100
    output_dir: str = "."
    headless: bool = True
    requests_per_minute: int = 20
    jitter_base: float = 1.0
    jitter_variance: float = 0.7
    dedup_threshold: float = 0.75
    browser_args: list[str] = field(default_factory=lambda: [
        "--lang=en-US", "--disable-blink-features=AutomationControlled"
    ])
    google_sheets: dict = field(default_factory=dict)  # { "enabled": false, "sheet_id": "", "credentials": "" }

    def __post_init__(self):
        if not self.maps_queries:
            self.maps_queries = self._default_maps_queries()
        if not self.facebook_queries:
            self.facebook_queries = self._default_fb_queries()
        if not self.instagram_queries:
            self.instagram_queries = self._default_ig_queries()

    def _default_maps_queries(self) -> list[str]:
        loc = self.location
        ind = self.industry
        if ind == "restaurant":
            return [
                f"restaurant {loc}",
                f"cafe {loc}",
                f"bar {loc}",
                f"bistro {loc}",
                f"eatery {loc}",
                f"food {loc}",
                f"dining {loc}",
            ]
        return [
            f"bed and breakfast {loc}",
            f"guesthouse {loc} {self.country}",
            f"boutique hotel {loc}",
            f"small hotel {loc}",
            f"bungalow resort {loc}",
            f"villa {loc}",
            f"homestay {loc}",
            f"resort {loc} cheap",
        ]

    def _default_fb_queries(self) -> list[str]:
        loc = self.location
        ind = self.industry
        if ind == "restaurant":
            return [
                f"ร้านอาหาร {loc}",
                f"คาเฟ่ {loc}",
                f"บาร์ {loc}",
                f"ร้านกาแฟ {loc}",
                f"{loc} restaurant",
                f"{loc} cafe",
                f"{loc} bar",
            ]
        return [
            f"ที่พัก {loc}",
            f"หอพัก {loc}",
            f"โรงแรมราคาถูก {loc}",
            f"บ้านพัก {loc}",
            f"รีสอร์ท {loc} ราคาถูก",
            f"เกสต์เฮ้าส์ {loc}",
            f"{loc} homestay",
            f"{loc} cheap guesthouse",
        ]

    def _default_ig_queries(self) -> list[str]:
        loc = self.location.lower().replace(" ", "")
        ind = self.industry
        if ind == "restaurant":
            return [
                f"#{loc}restaurant",
                f"#{loc}food",
                f"#{loc}cafe",
                f"#{loc}bar",
                f"#{loc}eats",
            ]
        return [
            f"#{loc}hotel",
            f"#{loc}resort",
            f"#{loc}villa",
            f"#{loc}guesthouse",
            f"#{loc}stay",
        ]

    @classmethod
    def from_dict(cls, d: dict) -> "ScraperConfig":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict:
        return asdict(self)


# ────────────────────────────────────────────────────────────
# SHARED HELPERS
# ────────────────────────────────────────────────────────────

async def check_for_booking(context, url: str, industry: str = "hotel") -> tuple[str, str]:
    """Check if a website has a direct booking engine."""
    if not url or not url.startswith("http"):
        return "no", "Invalid URL"
    page = await context.new_page()
    try:
        await page.goto(url, timeout=18000, wait_until="domcontentloaded")
        await jittered_sleep(1.5, 0.5)
        content = (await page.content()).lower()

        for signal in DIRECT_BOOKING_SIGNALS:
            if signal in content:
                return "yes", f"Signal: {signal}"

        ota_hits = [s for s in OTA_REDIRECT_SIGNALS if s in content]
        if ota_hits:
            return "no", f"OTA only: {', '.join(ota_hits[:2])}"

        return "no", "Static site, no booking"
    except PWTimeout:
        return "unknown", "Timeout loading site"
    except Exception as e:
        return "unknown", f"Error: {str(e)[:60]}"
    finally:
        await page.close()


async def scroll_feed(page, steps: int = 14, limiter: Optional[RateLimiter] = None):
    feed = await page.query_selector('div[role="feed"]')
    if not feed:
        return
    for _ in range(steps):
        await feed.evaluate("el => el.scrollTop += 800")
        if limiter:
            await limiter.acquire()
        else:
            await jittered_sleep(1.1, 0.3)


async def extract_maps_detail(page, industry: str = "hotel") -> dict | None:
    """Extract data from an open Google Maps detail panel."""
    name_el = await page.query_selector("h1")
    name = (await name_el.inner_text()).strip() if name_el else ""
    if not name:
        return None

    address, phone, rating, reviews, website = "", "", "", "", ""

    addr_btn = await page.query_selector('button[data-item-id="address"]')
    if addr_btn:
        address = (await addr_btn.get_attribute("aria-label") or "").replace("Address: ", "")

    for sel in ['button[data-item-id*="phone"]', 'button[aria-label*="Phone"]']:
        phone_btn = await page.query_selector(sel)
        if phone_btn:
            phone = (await phone_btn.get_attribute("aria-label") or "").replace("Phone: ", "")
            break

    for sel in ["div.F7nice > span > span[aria-hidden]", 'span[role="img"][aria-label*="star"]']:
        el = await page.query_selector(sel)
        if el:
            rating = (await el.inner_text()).strip() or (await el.get_attribute("aria-label") or "")
            if rating:
                break

    for sel in ['span[aria-label*="review"]', 'button[aria-label*="review"]']:
        el = await page.query_selector(sel)
        if el:
            reviews = (await el.get_attribute("aria-label") or "").strip()
            if reviews:
                break

    web_el = await page.query_selector('a[data-item-id="authority"]')
    if web_el:
        website = (await web_el.get_attribute("href") or "").strip()

    return {
        "Name": name, "Address": address, "Phone": phone,
        "Rating": rating, "Reviews": reviews, "Website": website,
    }


async def dismiss_consent(page):
    for btn_text in ["Accept all", "Reject all", "I agree"]:
        try:
            btn = page.get_by_role("button", name=btn_text)
            if await btn.is_visible():
                await btn.click()
                await jittered_sleep(0.8, 0.2)
                break
        except Exception:
            pass


async def dismiss_overlay(page):
    for selector in [
        'button[aria-label="Close"]', 'button:has-text("Accept")',
        'button:has-text("I Accept")', 'button:has-text("OK")',
        'button[aria-label="Dismiss sign-in info."]',
        'button:has-text("Sign in later")',
    ]:
        try:
            el = page.locator(selector).first
            if await el.is_visible(timeout=2000):
                await el.click()
                await jittered_sleep(0.6, 0.1)
                break
        except Exception:
            pass


def is_facebook_url(url: str) -> bool:
    return bool(url and ("facebook.com" in url or "fb.com" in url))


def is_instagram_url(url: str) -> bool:
    return bool(url and "instagram.com" in url)


# ────────────────────────────────────────────────────────────
# SOURCE: GOOGLE MAPS
# ────────────────────────────────────────────────────────────

async def scrape_maps_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    leads: list[Lead] = []
    page = await context.new_page()

    for query in config.maps_queries:
        url = f"https://www.google.com/maps/search/{quote_plus(query)}"
        print(f"\n[MAPS] {query}")

        try:
            await limiter.acquire()
            await page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(4.0, 1.0)
        except PWTimeout:
            print("  Load timeout — continuing")
            continue
        except Exception as e:
            print(f"  Navigation failed: {e}")
            continue

        await dismiss_consent(page)

        feed = await page.query_selector('div[role="feed"]')
        if not feed:
            print("  No feed found")
            continue

        await scroll_feed(page, steps=14, limiter=limiter)

        links = await page.query_selector_all('div[role="feed"] a[href*="/maps/place/"]')
        hrefs = []
        for link in links:
            href = await link.get_attribute("href")
            if href and href not in hrefs:
                hrefs.append(href)
        print(f"  {len(hrefs)} unique links")

        for i, href in enumerate(hrefs[:30]):
            try:
                await limiter.acquire()
                await page.goto(href, timeout=30000, wait_until="load")
                await jittered_sleep(2.5, 0.8)
                data = await extract_maps_detail(page, config.industry)
                if not data:
                    continue

                phone_norm = normalize_phone(data.get("Phone", ""), config.country)
                if deduper.is_duplicate(data["Name"], data.get("Address", ""), phone_norm):
                    continue

                deduper.add(data["Name"], data.get("Address", ""), phone_norm)

                lead = Lead(
                    name=data["Name"],
                    address=data.get("Address", ""),
                    phone=data.get("Phone", ""),
                    phone_normalized=phone_norm,
                    rating=data.get("Rating", ""),
                    reviews=data.get("Reviews", ""),
                    website=data.get("Website", ""),
                    source="Google Maps",
                    source_url=href,
                    location_query=query,
                    industry=config.industry,
                    country=config.country,
                )
                lead.compute_dedup_key()
                leads.append(lead)
                print(f"  [{i+1:02d}] {lead.name[:45]:<45} | {lead.website or 'NO WEBSITE'}")
            except Exception as e:
                print(f"  Error: {e}")
            await jittered_sleep(0.8, 0.3)

    await page.close()
    return leads


# ────────────────────────────────────────────────────────────
# SOURCE: BOOKING.COM
# ────────────────────────────────────────────────────────────

async def scrape_booking_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    if config.industry == "restaurant":
        return []  # Booking.com doesn't do restaurants

    leads: list[Lead] = []
    page = await context.new_page()

    loc_encoded = quote_plus(f"{config.location}, {config.province}, {config.country}")
    search_url = (
        f"https://www.booking.com/searchresults.html"
        f"?ss={loc_encoded}"
        f"&checkin=2026-06-10&checkout=2026-06-11"
        f"&group_adults=2&no_rooms=1&lang=en-us"
        f"&nflt=ht_id%3D220%3Bht_id%3D208"
    )

    print(f"\n[BOOKING] Searching {config.location}...")

    all_props = []
    for p in range(config.booking_pages):
        offset = p * 25
        url = search_url if offset == 0 else search_url + f"&offset={offset}"
        print(f"  [Page {p+1}] offset={offset}")

        try:
            await limiter.acquire()
            await page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(4.0, 1.0)
            await dismiss_overlay(page)
        except PWTimeout:
            print("    Timeout")
            continue

        cards = await page.query_selector_all('[data-testid="property-card"]')
        print(f"    {len(cards)} cards")

        for card in cards:
            try:
                name_el = await card.query_selector('[data-testid="title"]')
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    continue

                phone_norm = ""  # Booking list view doesn't show phone
                if deduper.is_duplicate(name):
                    continue

                link_el = await card.query_selector('[data-testid="title-link"]')
                href = (await link_el.get_attribute("href") or "").split("?")[0] if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.booking.com" + href

                rating = ""
                rating_el = await card.query_selector('[data-testid="review-score"] > div:first-child')
                if rating_el:
                    rating = (await rating_el.inner_text()).strip()

                reviews = ""
                reviews_el = await card.query_selector('[data-testid="review-score"] > div:last-child')
                if reviews_el:
                    reviews = (await reviews_el.inner_text()).strip()

                location = ""
                loc_el = await card.query_selector('[data-testid="address"]')
                if loc_el:
                    location = (await loc_el.inner_text()).strip()

                all_props.append({
                    "Name": name, "Rating": rating, "Reviews": reviews,
                    "Location": location, "Booking_URL": href,
                })
            except Exception:
                continue
        await jittered_sleep(1.5, 0.5)

    # Check each property page for external website
    print(f"\n[BOOKING] Checking {len(all_props)} property pages...")
    for i, prop in enumerate(all_props):
        website = ""
        if prop["Booking_URL"]:
            try:
                await limiter.acquire()
                await page.goto(prop["Booking_URL"], timeout=30000, wait_until="load")
                await jittered_sleep(3.0, 0.8)
                await dismiss_overlay(page)

                for sel in [
                    '[data-testid="header-hotel-website"]',
                    'a[data-testid*="website"]',
                    'a[class*="hp__hotel_header__see_website"]',
                ]:
                    el = await page.query_selector(sel)
                    if el:
                        href = await el.get_attribute("href") or ""
                        if href and "booking.com" not in href:
                            website = href
                            break

                if not website:
                    links = await page.query_selector_all('a[href^="http"]:not([href*="booking.com"])')
                    for link in links:
                        text = (await link.inner_text()).strip().lower()
                        aria = (await link.get_attribute("aria-label") or "").lower()
                        if any(kw in text + aria for kw in ("website", "official", "visit hotel", "hotel site")):
                            href = await link.get_attribute("href") or ""
                            if href and "booking.com" not in href:
                                website = href
                                break
            except Exception:
                pass

        deduper.add(prop["Name"])

        lead = Lead(
            name=prop["Name"],
            address=prop["Location"],
            rating=prop["Rating"],
            reviews=prop["Reviews"],
            website=website,
            source="Booking.com",
            source_url=prop["Booking_URL"],
            industry=config.industry,
            country=config.country,
        )
        lead.compute_dedup_key()
        leads.append(lead)
        print(f"  [{i+1:03d}] {lead.name[:45]:<45} | {website or 'NO WEBSITE'}")
        await jittered_sleep(0.5, 0.2)

    await page.close()
    return leads


# ────────────────────────────────────────────────────────────
# SOURCE: AGODA
# ────────────────────────────────────────────────────────────

async def scrape_agoda_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    if config.industry == "restaurant":
        return []

    leads: list[Lead] = []
    page = await context.new_page()

    loc_encoded = quote_plus(config.location)
    search_url = (
        f"https://www.agoda.com/search"
        f"?city={loc_encoded}"
        f"&checkIn=2026-06-10&checkOut=2026-06-11"
        f"&adults=2&rooms=1&locale=en-us"
    )

    print(f"\n[AGODA] Searching {config.location}...")

    all_props = []
    for p in range(config.agoda_pages):
        offset = p * 25
        url = search_url if offset == 0 else search_url + f"&page={p+1}"
        print(f"  [Page {p+1}]")

        try:
            await limiter.acquire()
            await page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(4.0, 1.0)
        except PWTimeout:
            print("    Timeout")
            continue

        # Agoda property cards
        cards = await page.query_selector_all('[data-selenium="hotel-item"]')
        if not cards:
            cards = await page.query_selector_all('[class*="PropertyCard"]')
        if not cards:
            cards = await page.query_selector_all('a[href*="/hotel/"]')

        print(f"    {len(cards)} cards")

        for card in cards:
            try:
                name_el = await card.query_selector('[data-selenium="hotel-name"]')
                if not name_el:
                    name_el = await card.query_selector('h3, [class*="name"]')
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    continue

                if deduper.is_duplicate(name):
                    continue

                link_el = await card.query_selector('a[href*="/hotel/"]')
                href = ""
                if link_el:
                    href = await link_el.get_attribute("href") or ""
                    if href and not href.startswith("http"):
                        href = "https://www.agoda.com" + href

                rating = ""
                rating_el = await card.query_selector('[data-selenium="rating"]')
                if rating_el:
                    rating = (await rating_el.inner_text()).strip()

                location = ""
                loc_el = await card.query_selector('[data-selenium="address"]')
                if loc_el:
                    location = (await loc_el.inner_text()).strip()

                all_props.append({"Name": name, "Rating": rating, "Location": location, "Agoda_URL": href})
            except Exception:
                continue
        await jittered_sleep(2.0, 0.5)

    # Check property pages for website
    print(f"\n[AGODA] Checking {len(all_props)} properties for websites...")
    for i, prop in enumerate(all_props):
        website = ""
        if prop["Agoda_URL"]:
            try:
                await limiter.acquire()
                await page.goto(prop["Agoda_URL"], timeout=30000, wait_until="load")
                await jittered_sleep(3.0, 0.8)

                # Look for official website link
                links = await page.query_selector_all('a[href^="http"]:not([href*="agoda.com"])')
                for link in links:
                    text = (await link.inner_text()).strip().lower()
                    if any(kw in text for kw in ("official", "website", "hotel website")):
                        href = await link.get_attribute("href") or ""
                        if href and "agoda.com" not in href:
                            website = href
                            break
            except Exception:
                pass

        deduper.add(prop["Name"])

        lead = Lead(
            name=prop["Name"],
            address=prop["Location"],
            rating=prop["Rating"],
            website=website,
            source="Agoda",
            source_url=prop["Agoda_URL"],
            industry=config.industry,
            country=config.country,
        )
        lead.compute_dedup_key()
        leads.append(lead)
        print(f"  [{i+1:03d}] {lead.name[:45]:<45} | {website or 'NO WEBSITE'}")
        await jittered_sleep(0.5, 0.2)

    await page.close()
    return leads


# ────────────────────────────────────────────────────────────
# SOURCE: TRIPADVISOR
# ────────────────────────────────────────────────────────────

async def scrape_tripadvisor_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    leads: list[Lead] = []
    page = await context.new_page()

    # Build search URL based on industry
    geo_id = "g297913"  # Default Koh Samui
    location_slug = config.location.replace(" ", "_") + "_" + config.province.replace(" ", "_") + "_Province"

    if config.industry == "restaurant":
        base_url = f"https://www.tripadvisor.com/Restaurants-{geo_id}-{location_slug}.html"
    else:
        base_url = f"https://www.tripadvisor.com/Hotels-{geo_id}-{location_slug}-Hotels.html"

    print(f"\n[TRIPADVISOR] {config.location} ({config.industry})...")

    property_urls = []
    for p in range(config.tripadvisor_pages):
        offset = p * 30
        url = base_url if offset == 0 else base_url.replace(".html", f"-oa{offset}.html")
        print(f"  [Page {p+1}] offset={offset}")

        try:
            await limiter.acquire()
            await page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(3.5, 0.8)
            await dismiss_overlay(page)
        except PWTimeout:
            print("    Timeout")
            continue

        title = await page.title()
        if "captcha" in title.lower() or "access denied" in title.lower():
            print("    BLOCKED")
            break

        links = await page.query_selector_all('a[href*="_Review"]')
        for link in links:
            href = await link.get_attribute("href")
            if not href:
                continue
            full = f"https://www.tripadvisor.com{href}" if href.startswith("/") else href
            clean = full.split("?")[0]
            if clean not in property_urls and "_Review" in clean:
                property_urls.append(full)

        print(f"    {len(property_urls)} total URLs so far")
        await jittered_sleep(1.5, 0.4)

    print(f"\n[TRIPADVISOR] Visiting {len(property_urls)} properties...")
    for i, url in enumerate(property_urls):
        try:
            await limiter.acquire()
            await page.goto(url, timeout=35000, wait_until="load")
            await jittered_sleep(2.5, 0.7)
            await dismiss_overlay(page)
        except PWTimeout:
            continue

        name, phone, rating, reviews, website = "", "", "", "", ""

        # JSON-LD
        scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in scripts:
            try:
                raw = await script.inner_text()
                data = json.loads(raw)
                if isinstance(data, list):
                    data = data[0]
                types = data.get("@type", "")
                if isinstance(types, list):
                    types = " ".join(types)
                valid_types = ("Hotel", "Lodging", "BedAndBreakfast", "Motel", "Hostel",
                               "Restaurant", "FoodEstablishment", "CafeOrCoffeeShop", "BarOrPub")
                if any(t in types for t in valid_types):
                    name = data.get("name", "")
                    phone = data.get("telephone", "")
                    same_as = data.get("sameAs", "")
                    if same_as and "tripadvisor" not in same_as.lower():
                        website = same_as
                    break
            except Exception:
                continue

        # DOM fallbacks
        if not name:
            for sel in ['h1[data-automation="mainH1"]', "h1"]:
                el = await page.query_selector(sel)
                if el:
                    name = (await el.inner_text()).strip()
                    if name:
                        break

        if not rating:
            for sel in ['span[class*="biGQs"][class*="rating"]', 'svg[aria-label*="of 5 bubbles"]', 'span[class*="ZDEqb"]']:
                el = await page.query_selector(sel)
                if el:
                    rating = (await el.get_attribute("aria-label") or await el.inner_text() or "").strip()
                    if rating:
                        break

        if not reviews:
            for sel in ['a[href*="#REVIEWS"]', 'span[class*="reviews"]']:
                el = await page.query_selector(sel)
                if el:
                    reviews = (await el.inner_text()).strip()
                    if reviews:
                        break

        if not website:
            ext_links = await page.query_selector_all('a[target="_blank"][href^="http"]')
            for link in ext_links:
                href = await link.get_attribute("href") or ""
                if href and "tripadvisor" not in href and "facebook" not in href:
                    text = (await link.inner_text()).strip().lower()
                    aria = (await link.get_attribute("aria-label") or "").lower()
                    if any(kw in text + aria for kw in ("website", "visit", "official")):
                        website = href
                        break

        if not name:
            continue

        phone_norm = normalize_phone(phone, config.country)
        if deduper.is_duplicate(name, "", phone_norm):
            continue
        deduper.add(name, "", phone_norm)

        lead = Lead(
            name=name, phone=phone, phone_normalized=phone_norm,
            rating=rating, reviews=reviews,
            website=website, source="TripAdvisor", source_url=url,
            industry=config.industry, country=config.country,
        )
        lead.compute_dedup_key()
        leads.append(lead)
        print(f"  [{i+1:03d}] {lead.name[:45]:<45} | {lead.website or 'NO WEBSITE'}")
        await jittered_sleep(0.7, 0.3)

    await page.close()
    return leads


# ────────────────────────────────────────────────────────────
# SOURCE: FACEBOOK (via Maps Thai queries)
# ────────────────────────────────────────────────────────────

async def scrape_facebook_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    leads: list[Lead] = []
    maps_page = await context.new_page()
    fb_page = await context.new_page()

    print(f"\n[FACEBOOK] Searching {config.industry} queries...")

    fb_properties = []
    for query in config.facebook_queries:
        url = f"https://www.google.com/maps/search/{quote_plus(query)}"
        print(f"  [QUERY] {query}")

        try:
            await limiter.acquire()
            await maps_page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(4.0, 1.0)
        except PWTimeout:
            continue

        await dismiss_consent(maps_page)

        feed = await maps_page.query_selector('div[role="feed"]')
        if not feed:
            continue

        await scroll_feed(maps_page, steps=14, limiter=limiter)

        links = await maps_page.query_selector_all('div[role="feed"] a[href*="/maps/place/"]')
        hrefs = []
        for link in links:
            href = await link.get_attribute("href")
            if href and href not in hrefs:
                hrefs.append(href)

        for i, href in enumerate(hrefs[:25]):
            try:
                await limiter.acquire()
                await maps_page.goto(href, timeout=30000, wait_until="load")
                await jittered_sleep(2.5, 0.7)
                data = await extract_maps_detail(maps_page, config.industry)
                if not data:
                    continue

                website = data.get("Website", "")
                if not is_facebook_url(website):
                    continue

                phone_norm = normalize_phone(data.get("Phone", ""), config.country)
                if deduper.is_duplicate(data["Name"], data.get("Address", ""), phone_norm):
                    continue
                deduper.add(data["Name"], data.get("Address", ""), phone_norm)

                fb_properties.append({
                    **data, "maps_url": href, "phone_normalized": phone_norm,
                })
                print(f"    [FB] {data['Name'][:42]:<42} | {website}")
            except Exception:
                pass
            await jittered_sleep(0.8, 0.3)

    print(f"\n[FACEBOOK] Visiting {len(fb_properties)} FB pages...")
    for i, prop in enumerate(fb_properties):
        fb_url = prop["Website"]
        external_website = ""

        try:
            await limiter.acquire()
            await fb_page.goto(fb_url, timeout=25000, wait_until="load")
            await jittered_sleep(2.5, 0.7)
            source = await fb_page.content()

            for pat in [
                r'"website"\s*:\s*"(https?://[^"]+)"',
                r'"WebPage"[^}]{0,200}"url"\s*:\s*"(https?://(?!(?:www\.)?facebook)[^"]+)"',
            ]:
                m = re.search(pat, source, re.IGNORECASE)
                if m:
                    candidate = m.group(1)
                    if "facebook.com" not in candidate and "fb.com" not in candidate:
                        external_website = candidate
                        break

            if not external_website:
                links = await fb_page.query_selector_all('a[href^="http"]:not([href*="facebook"]):not([href*="fb.com"])')
                for link in links:
                    href = await link.get_attribute("href") or ""
                    if href.startswith("http") and "facebook" not in href:
                        external_website = href
                        break
        except Exception:
            pass

        has_website = "yes" if external_website else "no"
        booking, note = "no", "FB-only presence"
        if external_website:
            booking, note = await check_for_booking(context, external_website, config.industry)

        lead = Lead(
            name=prop["Name"],
            address=prop.get("Address", ""),
            phone=prop.get("Phone", ""),
            phone_normalized=prop.get("phone_normalized", ""),
            rating=prop.get("Rating", ""),
            website=external_website,
            has_website=has_website,
            has_direct_booking=booking,
            notes=note,
            source="Facebook (via Maps)",
            source_url=fb_url,
            industry=config.industry,
            country=config.country,
        )
        lead.compute_dedup_key()

        symbol = "[Y]" if booking == "yes" else "[N]"
        print(f"  {symbol} {lead.name[:45]:<45} | ext: {external_website or 'none'} | {note}")

        if has_website == "no" or booking in ("no", "unknown"):
            leads.append(lead)

        await jittered_sleep(0.5, 0.2)

    await maps_page.close()
    await fb_page.close()
    return leads


# ────────────────────────────────────────────────────────────
# SOURCE: INSTAGRAM
# ────────────────────────────────────────────────────────────

async def scrape_instagram_source(config: ScraperConfig, context, deduper: Deduplicator, limiter: RateLimiter) -> list[Lead]:
    """
    Instagram scraper: Searches Google for Instagram pages of local businesses,
    then extracts contact info from their bio/website link.
    """
    leads: list[Lead] = []
    page = await context.new_page()

    print(f"\n[INSTAGRAM] Searching for {config.industry} Instagram pages...")

    # Search Google for Instagram pages
    search_queries = []
    for query in config.instagram_queries:
        search_queries.append(f"site:instagram.com {config.location} {config.industry}")

    ig_profiles = []
    for search_query in search_queries[:3]:  # Limit to avoid blocks
        url = f"https://www.google.com/search?q={quote_plus(search_query)}"
        try:
            await limiter.acquire()
            await page.goto(url, timeout=45000, wait_until="load")
            await jittered_sleep(3.0, 0.8)

            # Extract Instagram links from Google results
            links = await page.query_selector_all('a[href*="instagram.com/"]')
            for link in links:
                href = await link.get_attribute("href") or ""
                # Google wraps links
                if "/url?q=" in href:
                    match = re.search(r'/url\?q=(https?://[^&]+)', href)
                    if match:
                        href = match.group(1)

                if "instagram.com/p/" in href or "instagram.com/stories" in href:
                    continue  # Skip posts/stories
                if "/instagram.com/" in href:
                    username = href.split("/instagram.com/")[-1].split("/")[0].split("?")[0]
                    if username and username not in [p["username"] for p in ig_profiles]:
                        ig_profiles.append({"url": f"https://www.instagram.com/{username}/", "username": username})

            print(f"  Found {len(ig_profiles)} IG profiles so far")
        except Exception as e:
            print(f"  Search error: {e}")

    print(f"\n[INSTAGRAM] Visiting {len(ig_profiles)} profiles...")
    for i, profile in enumerate(ig_profiles[:20]):  # Limit to avoid blocks
        try:
            await limiter.acquire()
            await page.goto(profile["url"], timeout=25000, wait_until="load")
            await jittered_sleep(3.0, 1.0)

            # Extract bio and external link from page meta/JSON
            source = await page.content()

            # Try to get business name from title or meta
            name = ""
            title_match = re.search(r'<title>([^<]+)</title>', source, re.IGNORECASE)
            if title_match:
                name = title_match.group(1).replace("• Instagram photos and videos", "").strip()

            # Extract external link from bio
            external_link = ""
            link_match = re.search(r'"external_url":"(https?://[^"]+)"', source)
            if link_match:
                external_link = link_match.group(1)
            else:
                # Fallback: look for link in bio text
                bio_match = re.search(r'"biography":"([^"]*)"', source)
                if bio_match:
                    bio = bio_match.group(1)
                    url_in_bio = re.search(r'(https?://[^\s"]+)', bio)
                    if url_in_bio:
                        external_link = url_in_bio.group(1)

            # Extract phone from bio if present
            phone = ""
            phone_match = re.search(r'(\+?\d[\d\s\-\(\)]{7,20})', source)
            if phone_match:
                phone = phone_match.group(1).strip()

            if not name:
                name = profile["username"]

            phone_norm = normalize_phone(phone, config.country)
            if deduper.is_duplicate(name, "", phone_norm):
                continue
            deduper.add(name, "", phone_norm)

            has_website = "yes" if external_link else "no"
            booking, note = "no", "IG-only presence"
            if external_link:
                booking, note = await check_for_booking(context, external_link, config.industry)

            lead = Lead(
                name=name,
                phone=phone,
                phone_normalized=phone_norm,
                website=external_link,
                has_website=has_website,
                has_direct_booking=booking,
                notes=note,
                source="Instagram",
                source_url=profile["url"],
                industry=config.industry,
                country=config.country,
            )
            lead.compute_dedup_key()

            symbol = "[Y]" if booking == "yes" else "[N]"
            print(f"  [{i+1:02d}] {symbol} {lead.name[:45]:<45} | {external_link or 'NO LINK'}")

            if has_website == "no" or booking in ("no", "unknown"):
                leads.append(lead)

        except Exception as e:
            print(f"  Error on {profile['url']}: {e}")

        await jittered_sleep(2.0, 0.8)

    await page.close()
    return leads


# ────────────────────────────────────────────────────────────
# GOOGLE SHEETS EXPORT
# ────────────────────────────────────────────────────────────

def export_to_google_sheets(leads: list[Lead], config: ScraperConfig, sheet_name: str = "Leads") -> bool:
    """Export leads to Google Sheets using gspread."""
    if not config.google_sheets.get("enabled"):
        return False

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("\n[WARNING] gspread not installed. Run: pip install gspread google-auth")
        return False

    try:
        creds_path = config.google_sheets.get("credentials", "credentials.json")
        sheet_id = config.google_sheets.get("sheet_id", "")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        client = gspread.authorize(creds)

        if sheet_id:
            spreadsheet = client.open_by_key(sheet_id)
        else:
            spreadsheet = client.create(f"Lead Scraper — {config.location} {datetime.now().strftime('%Y-%m-%d')}")
            print(f"\n[GSHEETS] Created new sheet: {spreadsheet.url}")

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)

        # Headers
        headers = ["Name", "Address", "Phone", "Phone (Normalized)", "Rating", "Reviews",
                   "Website", "Has Website", "Has Direct Booking", "Notes", "Source",
                   "Source URL", "Industry", "Country", "Scraped At", "Dedup Key"]
        worksheet.append_row(headers)

        # Data rows
        for lead in leads:
            row = [
                lead.name, lead.address, lead.phone, lead.phone_normalized,
                lead.rating, lead.reviews, lead.website, lead.has_website,
                lead.has_direct_booking, lead.notes, lead.source,
                lead.source_url, lead.industry, lead.country,
                lead.scraped_at, lead.dedup_key,
            ]
            worksheet.append_row(row)

        print(f"[GSHEETS] Exported {len(leads)} leads to '{sheet_name}'")
        return True

    except Exception as e:
        print(f"[GSHEETS] Error: {e}")
        return False


# ────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ────────────────────────────────────────────────────────────

async def run_scraper(config: ScraperConfig) -> dict:
    """Run the full scraper pipeline and return results."""
    all_results: list[Lead] = []
    deduper = Deduplicator(name_threshold=config.dedup_threshold)
    limiter = RateLimiter(requests_per_minute=config.requests_per_minute)

    print(f"\n{'='*60}")
    print(f"  UNIVERSAL LEAD SCRAPER")
    print(f"  Location: {config.location}, {config.province}, {config.country}")
    print(f"  Industry: {config.industry}")
    print(f"  Sources:  {', '.join(config.sources)}")
    print(f"  Rate:     {config.requests_per_minute} req/min")
    print(f"  Jitter:   {config.jitter_base}±{config.jitter_variance}s")
    print(f"  Dedup:    threshold={config.dedup_threshold}")
    print(f"{'='*60}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=config.headless,
            args=config.browser_args,
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=UA,
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

        source_results = {}
        for source in config.sources:
            if source == "maps":
                source_results["maps"] = await scrape_maps_source(config, context, deduper, limiter)
                all_results.extend(source_results["maps"])
            elif source == "booking":
                source_results["booking"] = await scrape_booking_source(config, context, deduper, limiter)
                all_results.extend(source_results["booking"])
            elif source == "agoda":
                source_results["agoda"] = await scrape_agoda_source(config, context, deduper, limiter)
                all_results.extend(source_results["agoda"])
            elif source == "tripadvisor":
                source_results["tripadvisor"] = await scrape_tripadvisor_source(config, context, deduper, limiter)
                all_results.extend(source_results["tripadvisor"])
            elif source == "facebook":
                source_results["facebook"] = await scrape_facebook_source(config, context, deduper, limiter)
                all_results.extend(source_results["facebook"])
            elif source == "instagram":
                source_results["instagram"] = await scrape_instagram_source(config, context, deduper, limiter)
                all_results.extend(source_results["instagram"])

        # Booking check for all leads with websites
        print(f"\n[BOOKING CHECK] Checking {sum(1 for r in all_results if r.website)} websites...")
        for r in all_results:
            if r.website:
                r.has_website = "yes"
                booking, note = await check_for_booking(context, r.website, config.industry)
                r.has_direct_booking = booking
                r.notes = note
            else:
                r.has_website = "no"
                r.has_direct_booking = "no"
                r.notes = "No website listed"
            symbol = "[Y]" if r.has_direct_booking == "yes" else "[N]"
            print(f"  {symbol} {r.name[:45]:<45} -> {r.notes}")

        await browser.close()

    # Filter to leads
    leads = [
        r for r in all_results
        if r.has_website == "no" or r.has_direct_booking in ("no", "unknown")
    ]

    # Save outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{config.location.replace(' ', '_').lower()}_{config.industry}_{timestamp}"

    # JSON output
    json_path = os.path.join(config.output_dir, f"{base_name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "config": config.to_dict(),
            "summary": {
                "total_scraped": len(all_results),
                "total_leads": len(leads),
                "by_source": {k: len(v) for k, v in source_results.items()},
                "no_website": len([r for r in leads if r.has_website == "no"]),
                "website_no_booking": len([r for r in leads if r.has_website == "yes"]),
                "countries": list(set(r.country for r in all_results if r.country)),
                "industries": list(set(r.industry for r in all_results if r.industry)),
            },
            "all_results": [r.to_dict() for r in all_results],
            "leads": [r.to_dict() for r in leads],
        }, f, ensure_ascii=False, indent=2)

    # CSV output (leads only)
    csv_path = os.path.join(config.output_dir, f"{base_name}_leads.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "name", "address", "phone", "phone_normalized", "rating", "reviews",
            "website", "has_website", "has_direct_booking", "notes",
            "source", "source_url", "industry", "country", "scraped_at", "location_query", "dedup_key",
        ])
        writer.writeheader()
        writer.writerows([r.to_dict() for r in leads])

    # Full CSV
    full_csv_path = os.path.join(config.output_dir, f"{base_name}_all.csv")
    with open(full_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "name", "address", "phone", "phone_normalized", "rating", "reviews",
            "website", "has_website", "has_direct_booking", "notes",
            "source", "source_url", "industry", "country", "scraped_at", "location_query", "dedup_key",
        ])
        writer.writeheader()
        writer.writerows([r.to_dict() for r in all_results])

    # Google Sheets export
    sheets_ok = export_to_google_sheets(leads, config)

    print(f"\n{'='*60}")
    print(f"TOTAL scraped : {len(all_results)}")
    print(f"LEADS         : {len(leads)}")
    print(f"  - No website: {len([r for r in leads if r.has_website == 'no'])}")
    print(f"  - Site, no booking: {len([r for r in leads if r.has_website == 'yes'])}")
    print(f"DEDUPED       : {len(deduper.seen_names)} unique businesses")
    print(f"JSON saved    : {json_path}")
    print(f"CSV (leads)   : {csv_path}")
    print(f"CSV (all)     : {full_csv_path}")
    if sheets_ok:
        print(f"Google Sheets : Exported successfully")
    print(f"{'='*60}")

    return {
        "config": config.to_dict(),
        "total_scraped": len(all_results),
        "total_leads": len(leads),
        "by_source": {k: len(v) for k, v in source_results.items()},
        "json_path": json_path,
        "csv_path": csv_path,
        "full_csv_path": full_csv_path,
        "leads": [r.to_dict() for r in leads],
        "all_results": [r.to_dict() for r in all_results],
    }


# ────────────────────────────────────────────────────────────
# PRESETS
# ────────────────────────────────────────────────────────────

PRESETS = {
    "thailand_hotels": {
        "location": "Koh Samui", "country": "Thailand", "province": "Surat Thani",
        "industry": "hotel",
        "sources": ["maps", "booking", "tripadvisor", "facebook", "instagram", "agoda"],
    },
    "thailand_restaurants": {
        "location": "Koh Samui", "country": "Thailand", "province": "Surat Thani",
        "industry": "restaurant",
        "sources": ["maps", "tripadvisor", "facebook", "instagram"],
    },
    "bali_hotels": {
        "location": "Bali", "country": "Indonesia", "province": "Bali",
        "industry": "hotel",
        "sources": ["maps", "booking", "tripadvisor", "facebook", "instagram", "agoda"],
    },
    "bali_restaurants": {
        "location": "Bali", "country": "Indonesia", "province": "Bali",
        "industry": "restaurant",
        "sources": ["maps", "tripadvisor", "facebook", "instagram"],
    },
    "vietnam_hotels": {
        "location": "Da Nang", "country": "Vietnam", "province": "Da Nang",
        "industry": "hotel",
        "sources": ["maps", "booking", "tripadvisor", "facebook", "instagram", "agoda"],
    },
    "portugal_hotels": {
        "location": "Lisbon", "country": "Portugal", "province": "Lisbon",
        "industry": "hotel",
        "sources": ["maps", "booking", "tripadvisor", "facebook", "instagram", "agoda"],
    },
    "mexico_restaurants": {
        "location": "Tulum", "country": "Mexico", "province": "Quintana Roo",
        "industry": "restaurant",
        "sources": ["maps", "tripadvisor", "facebook", "instagram"],
    },
}


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Universal Lead Scraper")
    parser.add_argument("--config", type=str, help="Path to JSON config file")
    parser.add_argument("--preset", type=str, choices=list(PRESETS.keys()), help="Use a preset")
    parser.add_argument("--location", type=str, help="Location name")
    parser.add_argument("--country", type=str, default="Thailand")
    parser.add_argument("--province", type=str, default="")
    parser.add_argument("--industry", type=str, choices=["hotel", "restaurant"], default="hotel")
    parser.add_argument("--sources", type=str, help="Comma-separated: maps,booking,tripadvisor,facebook,instagram,agoda")
    parser.add_argument("--output", type=str, default=".", help="Output directory")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--rpm", type=int, default=20, help="Requests per minute limit")
    args = parser.parse_args()

    if args.config:
        with open(args.config, "r", encoding="utf-8") as f:
            config = ScraperConfig.from_dict(json.load(f))
    elif args.preset:
        config = ScraperConfig.from_dict(PRESETS[args.preset])
    else:
        kwargs = {
            "location": args.location or "Koh Samui",
            "country": args.country,
            "industry": args.industry,
        }
        if args.province:
            kwargs["province"] = args.province
        if args.sources:
            kwargs["sources"] = [s.strip() for s in args.sources.split(",")]
        kwargs["output_dir"] = args.output
        kwargs["headless"] = not args.no_headless
        kwargs["requests_per_minute"] = args.rpm
        config = ScraperConfig(**kwargs)

    result = asyncio.run(run_scraper(config))
    return result


from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os as _os

app = FastAPI(title="Universal Scraper API")

# Permettre au tableau de bord local (HTML) d'interroger l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# État global temporaire pour suivre la progression
scraper_status = {
    "running": False,
    "progress": 0,
    "leads_found": 0,
    "current_source": "Idle",
    "results": [],
    "summary": {},
}

class ScrapeRequest(BaseModel):
    location: str
    country: str
    industry: str
    sources: list

async def run_scraper_task(req: ScrapeRequest):
    global scraper_status
    scraper_status.update({
        "running": True,
        "progress": 5,
        "results": [],
        "leads_found": 0,
        "current_source": "Starting...",
    })
    try:
        config = ScraperConfig(
            location=req.location,
            country=req.country,
            industry=req.industry,
            sources=req.sources,
        )
        scraper_status["current_source"] = f"Scraping {', '.join(req.sources)}..."
        scraper_status["progress"] = 10
        result = await run_scraper(config)
        scraper_status["progress"] = 100
        scraper_status["current_source"] = "Finished"
        scraper_status["leads_found"] = result.get("total_leads", 0)
        scraper_status["results"] = result.get("all_results", [])
        scraper_status["summary"] = {
            "total_scraped": result.get("total_scraped", 0),
            "total_leads": result.get("total_leads", 0),
            "by_source": result.get("by_source", {}),
            "no_website": sum(1 for r in result.get("all_results", []) if r.get("has_website") == "no"),
            "website_no_booking": sum(1 for r in result.get("all_results", []) if r.get("has_website") == "yes" and r.get("has_direct_booking") != "yes"),
            "countries": list({r.get("country") for r in result.get("all_results", []) if r.get("country")}),
        }
    except Exception as e:
        scraper_status["current_source"] = f"Error: {str(e)}"
    finally:
        scraper_status["running"] = False

@app.post("/api/start")
def start_scraper(payload: ScrapeRequest, background_tasks: BackgroundTasks):
    if scraper_status["running"]:
        return {"status": "already_running"}
    background_tasks.add_task(run_scraper_task, payload)
    return {"status": "started"}

@app.get("/api/status")
def get_status():
    return scraper_status

@app.get("/")
def serve_dashboard():
    html_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "index.html")
    return FileResponse(html_path, media_type="text/html")

if __name__ == "__main__":
    if "--server" in sys.argv:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        main()