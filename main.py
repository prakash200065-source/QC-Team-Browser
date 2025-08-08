from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright
import uvicorn
import base64
import nest_asyncio
import asyncio
from typing import Optional
import logging
from urllib.parse import urljoin, urlparse
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

nest_asyncio.apply()

app = FastAPI(
    title="Advanced Web Scraper API",
    description="Complete web scraping API with error handling and comprehensive content extraction",
    version="1.0.0"
)

# Global browser instance for better performance
browser_instance = None

async def get_browser():
    global browser_instance
    if browser_instance is None:
        playwright = await async_playwright().start()
        browser_instance = await playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--disable-dev-tools']
        )
    return browser_instance

@app.on_event("startup")
async def startup_event():
    """Initialize browser on startup"""
    try:
        await get_browser()
        logger.info("Browser initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize browser: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Close browser on shutdown"""
    global browser_instance
    if browser_instance:
        await browser_instance.close()
        logger.info("Browser closed successfully")

def is_valid_url(url: str) -> bool:
    """Validate URL format"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def generate_markdown(content_list, metadata):
    """Convert extracted content to properly formatted markdown"""
    markdown_lines = []
    
    # Add metadata as frontmatter
    if metadata.get('title'):
        markdown_lines.append(f"# {metadata['title']}\n")
    
    if metadata.get('description'):
        markdown_lines.append(f"*{metadata['description']}*\n")
    
    if metadata.get('author'):
        markdown_lines.append(f"**Author:** {metadata['author']}\n")
    
    markdown_lines.append("---\n")
    
    for item in content_list:
        item_type = item.get('type', '')
        
        if item_type == 'heading':
            level = item.get('level', 'h1')
            level_num = int(level[1]) if level.startswith('h') else 1
            markdown_lines.append(f"{'#' * level_num} {item.get('text', '')}\n")
            
        elif item_type == 'paragraph':
            markdown_lines.append(f"{item.get('text', '')}\n")
            
        elif item_type == 'text':
            text = item.get('text', '')
            if len(text) > 20:  # Only add substantial text
                markdown_lines.append(f"{text}\n")
            
        elif item_type == 'link':
            text = item.get('text', '')
            href = item.get('href', '')
            markdown_lines.append(f"[{text}]({href})")
            
        elif item_type == 'anchor_link':
            text = item.get('text', '')
            href = item.get('href', '')
            # Extract anchor part
            anchor_part = href.split('#')[-1] if '#' in href else ''
            markdown_lines.append(f"[{text}](#{anchor_part}) *(anchor link)*")
            
        elif item_type == 'image':
            alt = item.get('alt', 'Image')
            src = item.get('src', '')
            markdown_lines.append(f"![{alt}]({src})")
            
        elif item_type == 'unordered_list':
            items = item.get('items', [])
            for list_item in items:
                markdown_lines.append(f"- {list_item}")
            markdown_lines.append("")  # Extra line after list
            
        elif item_type == 'ordered_list':
            items = item.get('items', [])
            for i, list_item in enumerate(items, 1):
                markdown_lines.append(f"{i}. {list_item}")
            markdown_lines.append("")  # Extra line after list
            
        elif item_type == 'table':
            rows = item.get('rows', [])
            if rows:
                # Header row
                markdown_lines.append("| " + " | ".join(rows[0]) + " |")
                # Separator
                markdown_lines.append("| " + " | ".join(["---"] * len(rows[0])) + " |")
                # Data rows
                for row in rows[1:]:
                    markdown_lines.append("| " + " | ".join(row) + " |")
                markdown_lines.append("")  # Extra line after table
                
        elif item_type == 'blockquote':
            text = item.get('text', '')
            markdown_lines.append(f"> {text}\n")
            
        elif item_type == 'code':
            text = item.get('text', '')
            language = item.get('language', '').split()[0] if item.get('language') else ''
            if '\n' in text:  # Multi-line code
                markdown_lines.append(f"```{language}")
                markdown_lines.append(text)
                markdown_lines.append("```\n")
            else:  # Inline code
                markdown_lines.append(f"`{text}`")
                
        elif item_type == 'button':
            text = item.get('text', '')
            markdown_lines.append(f"**[{text}]** *(button)*")
    
    # Add links section at the end
    all_links = []
    anchor_links = []
    
    for item in content_list:
        if item.get('type') == 'link':
            all_links.append(f"- [{item.get('text', '')}]({item.get('href', '')})")
        elif item.get('type') == 'anchor_link':
            text = item.get('text', '')
            href = item.get('href', '')
            anchor_part = href.split('#')[-1] if '#' in href else ''
            anchor_links.append(f"- [{text}](#{anchor_part})")
    
    if all_links or anchor_links:
        markdown_lines.append("\n---\n")
        markdown_lines.append("## Links Found\n")
        
        if all_links:
            markdown_lines.append("### External Links")
            markdown_lines.extend(all_links)
            markdown_lines.append("")
            
        if anchor_links:
            markdown_lines.append("### Anchor Links")
            markdown_lines.extend(anchor_links)
    
    return "\n".join(markdown_lines)


@app.get("/scrape")
async def scrape_page(
    url: str = Query(..., description="URL of the page to scrape"),
    timeout: int = Query(30, description="Timeout in seconds (default: 30)"),
    wait_time: int = Query(3, description="Additional wait time in seconds (default: 3)"),
    full_screenshot: bool = Query(True, description="Take full page screenshot (default: True)")
):
    """
    Scrape webpage and extract all visible content with comprehensive error handling
    """
    
    # Input validation
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")
    
    if not is_valid_url(url):
        raise HTTPException(status_code=400, detail="Invalid URL format")
    
    if timeout < 5 or timeout > 120:
        raise HTTPException(status_code=400, detail="Timeout must be between 5 and 120 seconds")
    
    start_time = time.time()
    page = None
    
    try:
        browser = await get_browser()
        
        # Create new page with error handling
        try:
            page = await browser.new_page()
            await page.set_viewport_size({"width": 1920, "height": 1080})
            
            # Set reasonable timeouts
            page.set_default_timeout(timeout * 1000)
            page.set_default_navigation_timeout(timeout * 1000)
            
        except Exception as e:
            logger.error(f"Failed to create page: {e}")
            raise HTTPException(status_code=500, detail="Failed to initialize browser page")

        # Navigate to URL with comprehensive error handling
        try:
            logger.info(f"Navigating to: {url}")
            response = await page.goto(
                url, 
                wait_until="domcontentloaded",
                timeout=timeout * 1000
            )
            
            if response is None:
                raise HTTPException(status_code=400, detail="Failed to load the page - no response received")
            
            if response.status >= 400:
                raise HTTPException(status_code=response.status, detail=f"HTTP {response.status}: {response.status_text}")
                
        except asyncio.TimeoutError:
            raise HTTPException(status_code=408, detail=f"Page load timeout after {timeout} seconds")
        except Exception as e:
            logger.error(f"Navigation error: {e}")
            raise HTTPException(status_code=500, detail=f"Navigation failed: {str(e)}")

        # Wait for additional content to load
        try:
            await asyncio.sleep(wait_time)
            await page.wait_for_load_state("networkidle", timeout=10000)
        except asyncio.TimeoutError:
            logger.warning("Network idle timeout - proceeding with extraction")
        except Exception as e:
            logger.warning(f"Load state warning: {e}")

        # Take screenshot with error handling
        screenshot_base64 = ""
        try:
            if full_screenshot:
                screenshot = await page.screenshot(
                    full_page=True,
                    timeout=15000
                )
                screenshot_base64 = base64.b64encode(screenshot).decode("utf-8")
            else:
                screenshot = await page.screenshot(timeout=15000)
                screenshot_base64 = base64.b64encode(screenshot).decode("utf-8")
        except Exception as e:
            logger.warning(f"Screenshot failed: {e}")
            screenshot_base64 = ""

        # Extract comprehensive content
        try:
            extracted_data = await page.evaluate("""
                () => {
                    function isElementVisible(el) {
                        if (!el || el.nodeType !== Node.ELEMENT_NODE) return false;
                        
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        
                        return (
                            style.display !== 'none' &&
                            style.visibility !== 'hidden' &&
                            style.opacity !== '0' &&
                            rect.width > 0 &&
                            rect.height > 0 &&
                            rect.top < window.innerHeight &&
                            rect.bottom > 0 &&
                            rect.left < window.innerWidth &&
                            rect.right > 0
                        );
                    }

                    function getElementPosition(el) {
                        const rect = el.getBoundingClientRect();
                        return {
                            top: rect.top + window.scrollY,
                            left: rect.left + window.scrollX
                        };
                    }

                    function extractComprehensiveContent() {
                        let results = [];
                        let processedElements = new Set();

                        // Get all elements in DOM order
                        const allElements = document.querySelectorAll('*');
                        
                        for (let el of allElements) {
                            if (!isElementVisible(el) || processedElements.has(el)) continue;
                            
                            const tag = el.tagName.toLowerCase();
                            const position = getElementPosition(el);
                            let extracted = false;

                            // Headings
                            if (tag.match(/^h[1-6]$/)) {
                                const text = el.innerText?.trim();
                                if (text) {
                                    results.push({
                                        type: "heading",
                                        level: tag,
                                        text: text,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Paragraphs
                            else if (tag === 'p') {
                                const text = el.innerText?.trim();
                                if (text) {
                                    results.push({
                                        type: "paragraph",
                                        text: text,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Plain text in divs, spans (only if no child block elements)
                            else if (['div', 'span', 'section', 'article'].includes(tag)) {
                                const hasBlockChildren = el.querySelector('p, h1, h2, h3, h4, h5, h6, ul, ol, table, blockquote');
                                if (!hasBlockChildren) {
                                    const text = el.innerText?.trim();
                                    if (text && text.length > 3) {
                                        results.push({
                                            type: "text",
                                            text: text,
                                            tag: tag,
                                            position: position
                                        });
                                        extracted = true;
                                    }
                                }
                            }
                            
                            // Links (including anchor links with proper text extraction)
                            else if (tag === 'a') {
                                const text = el.innerText?.trim();
                                const href = el.href;
                                if (text && href) {
                                    const isAnchorLink = href.includes('#');
                                    const anchorText = isAnchorLink ? href.split('#')[1] || '' : '';
                                    
                                    results.push({
                                        type: isAnchorLink ? "anchor_link" : "link",
                                        text: text,
                                        href: href,
                                        anchor_text: isAnchorLink ? anchorText : null,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Images
                            else if (tag === 'img') {
                                const src = el.src;
                                const alt = el.alt;
                                if (src) {
                                    results.push({
                                        type: "image",
                                        src: src,
                                        alt: alt || "",
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Lists
                            else if (['ul', 'ol'].includes(tag)) {
                                const items = Array.from(el.querySelectorAll('li'))
                                    .map(li => li.innerText?.trim())
                                    .filter(Boolean);
                                if (items.length > 0) {
                                    results.push({
                                        type: tag === 'ul' ? "unordered_list" : "ordered_list",
                                        items: items,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Tables
                            else if (tag === 'table') {
                                const rows = [];
                                const tableRows = el.querySelectorAll('tr');
                                for (let row of tableRows) {
                                    const cells = Array.from(row.querySelectorAll('td, th'))
                                        .map(cell => cell.innerText?.trim())
                                        .filter(Boolean);
                                    if (cells.length > 0) {
                                        rows.push(cells);
                                    }
                                }
                                if (rows.length > 0) {
                                    results.push({
                                        type: "table",
                                        rows: rows,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Blockquotes
                            else if (tag === 'blockquote') {
                                const text = el.innerText?.trim();
                                if (text) {
                                    results.push({
                                        type: "blockquote",
                                        text: text,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Code blocks
                            else if (['pre', 'code'].includes(tag)) {
                                const text = el.innerText?.trim();
                                if (text) {
                                    results.push({
                                        type: "code",
                                        text: text,
                                        language: el.className || "",
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }
                            
                            // Buttons
                            else if (['button', 'input'].includes(tag)) {
                                const text = el.innerText?.trim() || el.value?.trim();
                                if (text) {
                                    results.push({
                                        type: "button",
                                        text: text,
                                        position: position
                                    });
                                    extracted = true;
                                }
                            }

                            if (extracted) {
                                processedElements.add(el);
                            }
                        }

                        // Sort by position (top to bottom, left to right)
                        results.sort((a, b) => {
                            if (Math.abs(a.position.top - b.position.top) < 10) {
                                return a.position.left - b.position.left;
                            }
                            return a.position.top - b.position.top;
                        });

                        return results;
                    }

                    // Extract page metadata
                    const metadata = {
                        title: document.title || "",
                        description: document.querySelector('meta[name="description"]')?.content || "",
                        keywords: document.querySelector('meta[name="keywords"]')?.content || "",
                        author: document.querySelector('meta[name="author"]')?.content || "",
                        canonical: document.querySelector('link[rel="canonical"]')?.href || "",
                        language: document.documentElement.lang || ""
                    };

                    const content = extractComprehensiveContent();
                    
                    return {
                        content: content,
                        metadata: metadata,
                        stats: {
                            total_elements: content.length,
                            headings: content.filter(item => item.type === 'heading').length,
                            paragraphs: content.filter(item => item.type === 'paragraph').length,
                            links: content.filter(item => item.type === 'link').length,
                            anchor_links: content.filter(item => item.type === 'anchor_link').length,
                            images: content.filter(item => item.type === 'image').length,
                            tables: content.filter(item => item.type === 'table').length
                        }
                    };
                }
            """)

        except Exception as e:
            logger.error(f"Content extraction failed: {e}")
            raise HTTPException(status_code=500, detail=f"Content extraction failed: {str(e)}")

        # Process extracted data
        content = extracted_data.get('content', [])
        metadata = extracted_data.get('metadata', {})
        stats = extracted_data.get('stats', {})

        # Extract separate lists for compatibility
        links = [item['href'] for item in content if item.get('type') in ['link', 'anchor_link']]
        images = [item['src'] for item in content if item.get('type') == 'image']
        anchor_links = [item['href'] for item in content if item.get('type') == 'anchor_link']

        processing_time = round(time.time() - start_time, 2)

        # Generate markdown formatted content
        markdown_content = generate_markdown(content, metadata)
        
        result = {
            "success": True,
            "url": url,
            "processing_time_seconds": processing_time,
            "metadata": metadata,
            "statistics": stats,
            "visible_content": content,
            "markdown_content": markdown_content,
            "links": links,
            "anchor_links": anchor_links,
            "images": images,
            "screenshot_base64": screenshot_base64,
            "timestamp": time.time()
        }

        logger.info(f"Successfully scraped {url} in {processing_time}s - Found {len(content)} elements")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    finally:
        # Clean up page
        if page:
            try:
                await page.close()
            except Exception as e:
                logger.warning(f"Page cleanup warning: {e}")

@app.get("/markdown")
async def scrape_as_markdown(
    url: str = Query(..., description="URL of the page to scrape"),
    timeout: int = Query(30, description="Timeout in seconds (default: 30)"),
    include_images: bool = Query(True, description="Include images in markdown (default: True)")
):
    """
    Scrape webpage and return content as clean markdown format
    """
    try:
        # Use the main scrape function
        result = await scrape_page(url, timeout, 3, False)  # No screenshot for markdown-only
        
        if result.get("success"):
            return {
                "success": True,
                "url": url,
                "markdown": result["markdown_content"],
                "word_count": len(result["markdown_content"].split()),
                "processing_time": result["processing_time_seconds"]
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to scrape page")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating markdown: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "browser_ready": browser_instance is not None
    }

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Advanced Web Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "scrape": "/scrape?url=<URL>&timeout=30&wait_time=3&full_screenshot=true",
            "health": "/health",
            "docs": "/docs"
        },
        "features": [
            "Comprehensive content extraction",
            "Error handling and validation", 
            "Anchor links support",
            "Plain text extraction",
            "Table data extraction",
            "Metadata extraction",
            "Performance statistics",
            "Full page screenshots"
        ]
    }

if __name__ == "__main__":
    # Get port from environment variable (Render assigns this)
    PORT = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=PORT,
        reload=False,
        access_log=True
    )