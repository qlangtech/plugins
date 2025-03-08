import com.microsoft.playwright.*;
import com.microsoft.playwright.options.BoundingBox;

import java.nio.file.Paths;

public class ElementScreenshot {
    public static void main(String[] args) {
        try (Playwright playwright = Playwright.create()) {
            Browser browser = playwright.chromium().launch();
            Page page = browser.newPage();
            page.navigate("https://example.com");

            // 定位目标 div
            ElementHandle divElement = page.querySelector("div#target-id");
            if (divElement != null) {
                // 获取元素的边界框
                BoundingBox box = divElement.boundingBox();
                if (box != null) {
                    // 截取该区域
                    page.screenshot(new Page.ScreenshotOptions()
                            .setPath(Paths.get("div-screenshot.png"))
                            .setClip(box.x, box.y, box.width, box.height));
                }
            }
            browser.close();
        }
    }
}