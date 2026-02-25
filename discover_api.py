"""Discover API endpoints used by Global House store finder"""
import asyncio
import json
from playwright.async_api import async_playwright


async def discover():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        captured = []

        async def on_response(response):
            url = response.url
            if any(k in url.lower() for k in ["store", "branch", "shop", "api", "สาขา", "json"]):
                try:
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type:
                        body = await response.json()
                        captured.append({"url": url, "data": body})
                        print(f"\n[JSON API] {url}")
                        print(json.dumps(body, ensure_ascii=False, indent=2)[:500])
                except Exception as e:
                    pass

        async def on_request(request):
            url = request.url
            if any(k in url.lower() for k in ["store", "branch", "shop", "api", "สาขา"]):
                print(f"[REQ] {request.method} {url}")

        page.on("response", on_response)
        page.on("request", on_request)

        print("Loading store finder page...")
        await page.goto("https://globalhouse.co.th/store-finder", wait_until="networkidle", timeout=30000)

        # Also intercept ALL json responses
        print("\n\nCapturing ALL responses...")
        captured_all = []

        async def on_all_response(response):
            url = response.url
            try:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    body = await response.json()
                    captured_all.append({"url": url, "data": body})
                    if isinstance(body, list) and len(body) > 5:
                        print(f"\n[POTENTIAL DATA] {url} - array of {len(body)} items")
                        print(json.dumps(body[0], ensure_ascii=False, indent=2)[:300])
                    elif isinstance(body, dict):
                        keys = list(body.keys())
                        print(f"[JSON] {url} - keys: {keys[:10]}")
            except Exception:
                pass

        page.on("response", on_all_response)

        # Reload to catch everything
        await page.reload(wait_until="networkidle", timeout=30000)

        # Wait a bit more
        await asyncio.sleep(3)

        print("\n\nAll captured JSON endpoints:")
        for item in captured_all:
            print(f"  {item['url']}")

        await browser.close()


asyncio.run(discover())
