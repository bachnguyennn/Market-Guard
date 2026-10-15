import argparse
from pathlib import Path


def main() -> None:
    from playwright.sync_api import expect, sync_playwright

    parser = argparse.ArgumentParser(description="Capture the running MarketGuard dashboard.")
    parser.add_argument("--url", default="http://127.0.0.1:8502")
    parser.add_argument("--output-dir", default="docs/screenshots")
    parser.add_argument("--browser-executable")
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=args.browser_executable)
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
            page.goto(args.url, wait_until="networkidle")
            expect(page.get_by_role("heading", name="MarketGuard", exact=True)).to_be_visible(timeout=30000)
            page.get_by_role("tab", name="Market Overview", exact=True).click()
            expect(page.get_by_text("Total trades", exact=True)).to_be_visible()
            expect(page.locator('[data-testid="stDataFrame"]:visible canvas').first).to_be_visible(timeout=30000)
            page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
            page.screenshot(path=str(output_dir / "market-overview.png"), full_page=True)
            page.get_by_role("tab", name="Anomaly Alerts", exact=True).click()
            expect(page.get_by_text("Total alerts", exact=True)).to_be_visible()
            expect(page.locator('[data-testid="stDataFrame"]:visible canvas').first).to_be_visible(timeout=30000)
            page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
            page.screenshot(path=str(output_dir / "anomaly-alerts.png"), full_page=True)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
