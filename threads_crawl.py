from playwright.sync_api import sync_playwright

def extract_threads_post_by_class(profile_url, max_posts=20):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(profile_url)
        page.wait_for_timeout(5000)  # Let initial posts load

        # Post wrapper selector (1 post each)
        wrapper_selector = ".xrvj5dj.xd0jker.x1evr45z"
        post_wrappers = page.locator(wrapper_selector)
        total = post_wrappers.count()

        print(f"\n🧵 Extracting up to {total} loaded post(s)...\n")
        posts = []

        for i in range(total):
            wrapper = post_wrappers.nth(i)

            # 1. Get post text from the content block
            text_block = wrapper.locator(".x1a6qonq.x6ikm8r.x10wlt62.xj0a0fe.x126k92a.x6prxxf.x7r5mf7")
            spans = text_block.locator("span")
            lines = [
                spans.nth(j).inner_text().strip()
                for j in range(0,spans.count(),2)
                if spans.nth(j).inner_text().strip()
            ]
            post_text = " ".join(lines)

            # 2. Get post link from the anchor
            link_block = wrapper.locator(
                ".x1i10hfl.xjbqb8w.x1ejq31n.x18oe1m7.x1sy0etr.xstzfhl.x972fbf."
                "x10w94by.x1qhh985.x14e42zd.x9f619.x1ypdohk.xt0psk2.xe8uvvx.xdj266r."
                "x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl.x16tdsg8."
                "x1hl2dhg.xggy1nq.x1a2a7pz.x1lku1pv.x12rw4y6.xrkepyr.x1citr7e.x37wo2f"
            )
            href = link_block.first.get_attribute("href").split('/')[-1]

            if post_text:
                posts.append({
                    "id": href,
                    "text": post_text
                    
                })
            import datetime,csv
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            csv_filename = f"data/threads_posts_{current_date}.csv"

        # Write posts to CSV
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=['id',"text"])
                writer.writeheader()
                for post in posts:
                    writer.writerow(post)
       

        # Print results
        for i, post in enumerate(posts, 1):
            print("─" * 80)
            print(f"Post #{i}")
            print(post["text"])
            print(f"🔗 Link: {post['id']}")
        
        browser.close()
# Example profile


# Example post URL
extract_threads_post_by_class("https://www.threads.com/search?q=disaster&serp_type=default")
