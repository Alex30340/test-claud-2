import os
import logging
import time
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

_MESA_LIB_PATH = "/nix/store/24w3s75aa2lrvvxsybficn8y3zxd27kp-mesa-libgbm-25.1.0/lib"
if os.path.isdir(_MESA_LIB_PATH):
    existing = os.environ.get("LD_LIBRARY_PATH", "")
    if _MESA_LIB_PATH not in existing:
        os.environ["LD_LIBRARY_PATH"] = f"{_MESA_LIB_PATH}:{existing}" if existing else _MESA_LIB_PATH

COOKIE_BUTTON_SELECTORS = [
    "button:has-text('Accepter')",
    "button:has-text('Accept all')",
    "button:has-text('Accept')",
    "button:has-text('J\\'accepte')",
    "button:has-text('Tout accepter')",
    "button:has-text('OK')",
    "button:has-text('Continuer')",
    "[id*='cookie'] button",
    "[class*='cookie'] button",
    "[id*='consent'] button",
    "[class*='consent'] button",
    "[id*='gdpr'] button",
    "[class*='gdpr'] button",
    ".cc-accept",
    ".cc-allow",
    "#onetrust-accept-btn-handler",
    ".didomi-continue-without-agreeing",
]

TAB_KEYWORDS = [
    "composition", "ingrédient", "ingredient", "ingredients",
    "nutrition", "nutritionnel", "nutritionnelle", "valeurs nutritionnelles",
    "valeur nutritive", "analyse nutritionnelle", "aminogramme",
    "amino", "description", "détail", "detail", "détails", "details",
    "voir plus", "voir tout", "en savoir plus", "lire la suite",
    "afficher plus", "show more", "read more",
    "informations", "caractéristiques", "fiche technique",
]

GALLERY_SELECTORS = [
    ".product-gallery img",
    ".product-images img",
    ".product-media img",
    "[data-gallery] img",
    "[data-product-image]",
    ".swiper-slide img",
    ".slick-slide img",
    ".carousel-item img",
    ".product-single__photos img",
    ".product__media img",
    ".product-thumbnails img",
    ".thumbnail-list img",
    ".product-photo-container img",
    "[class*='gallery'] img",
    "[class*='carousel'] img",
    "[class*='slider'] img",
    "[class*='product-image'] img",
    "img[data-zoom]",
    "img[data-large]",
    "img[data-full]",
    "img[data-srcset]",
]

THUMBNAIL_NAV_SELECTORS = [
    ".product-thumbnails button",
    ".product-thumbnails a",
    ".product-thumbnails img",
    ".thumbnail-list button",
    ".thumbnail-list a",
    "[class*='thumb'] img",
    "[class*='thumb'] button",
    ".carousel-indicators button",
    ".carousel-indicators li",
    ".slick-dots button",
    ".swiper-pagination-bullet",
    "button[aria-label*='slide']",
    "button[aria-label*='image']",
]


def _dismiss_cookies(page):
    for selector in COOKIE_BUTTON_SELECTORS:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=500):
                btn.click(timeout=2000)
                logger.debug(f"[BROWSER] Dismissed cookie banner: {selector}")
                page.wait_for_timeout(300)
                return True
        except Exception:
            continue
    return False


def _expand_accordions(page):
    expanded = 0

    for kw in TAB_KEYWORDS:
        try:
            candidates = page.locator(
                f"button:has-text('{kw}'), "
                f"[role='tab']:has-text('{kw}'), "
                f"a:has-text('{kw}'), "
                f"summary:has-text('{kw}'), "
                f"[data-toggle]:has-text('{kw}'), "
                f"[class*='accordion']:has-text('{kw}'), "
                f"[class*='tab']:has-text('{kw}'), "
                f"h3:has-text('{kw}'), "
                f"h4:has-text('{kw}'), "
                f"div[class*='collapse']:has-text('{kw}')"
            )
            count = candidates.count()
            for i in range(min(count, 3)):
                try:
                    el = candidates.nth(i)
                    if el.is_visible(timeout=300):
                        el.click(timeout=1000, force=True)
                        expanded += 1
                        page.wait_for_timeout(200)
                except Exception:
                    continue
        except Exception:
            continue

    try:
        details_elements = page.locator("details:not([open])")
        count = details_elements.count()
        for i in range(min(count, 10)):
            try:
                el = details_elements.nth(i)
                summary = el.locator("summary").first
                if summary.is_visible(timeout=300):
                    summary.click(timeout=1000)
                    expanded += 1
                    page.wait_for_timeout(100)
            except Exception:
                continue
    except Exception:
        pass

    if expanded > 0:
        logger.info(f"[BROWSER] Expanded {expanded} accordion/tab elements")
    return expanded


def _scroll_page(page):
    try:
        page.evaluate("""
            () => {
                return new Promise((resolve) => {
                    let totalHeight = 0;
                    const distance = 400;
                    const timer = setInterval(() => {
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if (totalHeight >= document.body.scrollHeight || totalHeight > 5000) {
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            resolve();
                        }
                    }, 100);
                });
            }
        """)
        page.wait_for_timeout(500)
    except Exception as e:
        logger.debug(f"[BROWSER] Scroll error: {e}")


def _extract_all_images(page, page_url):
    images = set()

    for selector in GALLERY_SELECTORS:
        try:
            elements = page.locator(selector)
            count = elements.count()
            for i in range(min(count, 20)):
                try:
                    el = elements.nth(i)
                    for attr in ["src", "data-src", "data-lazy-src", "data-zoom", "data-large", "data-full", "data-original"]:
                        val = el.get_attribute(attr)
                        if val and not any(skip in val.lower() for skip in ["icon", "logo", "avatar", "pixel", ".svg", "tracking", "1x1", "spacer"]):
                            full = urljoin(page_url, val)
                            if full.startswith("http"):
                                images.add(full)
                    srcset = el.get_attribute("srcset")
                    if srcset:
                        parts = srcset.split(",")
                        for part in parts:
                            src = part.strip().split(" ")[0]
                            if src:
                                full = urljoin(page_url, src)
                                if full.startswith("http"):
                                    images.add(full)
                except Exception:
                    continue
        except Exception:
            continue

    return list(images)[:30]


def _click_thumbnails(page, page_url):
    extra_images = set()
    for selector in THUMBNAIL_NAV_SELECTORS:
        try:
            thumbs = page.locator(selector)
            count = thumbs.count()
            if count <= 1:
                continue
            for i in range(min(count, 8)):
                try:
                    thumb = thumbs.nth(i)
                    if thumb.is_visible(timeout=300):
                        thumb.click(timeout=1000, force=True)
                        page.wait_for_timeout(300)
                        main_img = page.locator(".product-gallery img, .product-images img, [class*='product'] img").first
                        src = main_img.get_attribute("src") or ""
                        if src:
                            full = urljoin(page_url, src)
                            if full.startswith("http"):
                                extra_images.add(full)
                except Exception:
                    continue
            if extra_images:
                break
        except Exception:
            continue
    return list(extra_images)


def fetch_page_with_browser(url: str, timeout_ms: int = 20000) -> dict | None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("[BROWSER] Playwright not installed")
        return None

    result = {
        "html": "",
        "images": [],
        "interactions": 0,
        "success": False,
    }

    pw = None
    browser = None
    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
            ]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="fr-FR",
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            }
        )
        page = context.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        _dismiss_cookies(page)

        _scroll_page(page)

        interactions = _expand_accordions(page)

        page.wait_for_timeout(500)

        all_images = _extract_all_images(page, url)

        thumb_images = _click_thumbnails(page, url)
        for img in thumb_images:
            if img not in all_images:
                all_images.append(img)

        html = page.content()

        result["html"] = html
        result["images"] = all_images
        result["interactions"] = interactions
        result["success"] = True

        logger.info(f"[BROWSER] Fetched {url}: {len(html)} chars, {len(all_images)} images, {interactions} interactions")

    except Exception as e:
        logger.warning(f"[BROWSER] Error fetching {url}: {e}")

    finally:
        try:
            if 'page' in dir() and page:
                page.close()
        except Exception:
            pass
        try:
            if 'context' in dir() and context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if pw:
                pw.stop()
        except Exception:
            pass

    return result if result["success"] else None
