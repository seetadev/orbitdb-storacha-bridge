import { expect, test } from "@playwright/test";

test("home page has expected h1", async ({ page }) => {
  await page.goto("/", { waitUntil: "domcontentloaded" });
  // Rely on the heading becoming visible instead of networkidle (which can hang in CI).
  await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
});
