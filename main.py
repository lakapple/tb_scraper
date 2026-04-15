import argparse
import json
import os
import threading
import time
from pathlib import Path

import pandas as pd
from DrissionPage import ChromiumOptions, ChromiumPage
from tqdm import tqdm


class Scraper:
    def __init__(self, schema_path, output_path, workers):
        with open(schema_path, "r", encoding="utf-8") as f:
            self.cfg = json.load(f)

        self.output_path = output_path or f"{Path(schema_path).stem}.xlsx"
        self.workers = workers
        self.results = []
        self.lock = threading.Lock()

        # Threading control
        self.stop_event = threading.Event()
        self.active_threads = []

        self.base_url = "https://tracuu.tranbien.edu.vn/diem-thi"

    def scrape_batch(self, item_list, pbar):
        failed = []
        self.active_threads = []

        # Partitioning logic for multiple workers
        for i in range(self.workers):
            worker_list = item_list[i :: self.workers]
            if not worker_list:
                continue

            t = threading.Thread(
                target=self.worker_thread, args=(worker_list, failed, pbar, i)
            )
            self.active_threads.append(t)
            t.start()

        # Responsive join: allows the main thread to catch KeyboardInterrupt quickly
        for t in self.active_threads:
            while t.is_alive():
                t.join(0.5)

        return failed

    def worker_thread(self, items, failed_list, pbar, worker_id):
        co = ChromiumOptions()
        co.incognito()
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-blink-features=AutomationControlled")
        co.set_argument("--disable-extensions")
        co.set_argument("--disable-images")
        co.headless() # Un-comment to hide the browser windows

        port = 9222 + worker_id + 1
        profile_path = os.path.abspath(f"./tmp_profiles/worker_{worker_id}")

        co.set_local_port(port)
        co.set_user_data_path(profile_path)

        page = ChromiumPage(co)

        try:
            page.get(self.base_url)
            page.listen.start("api/scores/search")

            for grade, sbd in items:
                # 🛑 Check if Ctrl+C was pressed
                if self.stop_event.is_set():
                    break

                success = self.process_sbd(page, grade, str(sbd))
                if not success and not self.stop_event.is_set():
                    failed_list.append((grade, sbd))
                    page.refresh()
                    time.sleep(1.5)

                # Update progress bar only if thread wasn't stopped
                if not self.stop_event.is_set():
                    pbar.update(1)
        except Exception as e:
            pass
        finally:
            # 🛑 Guarantee browser closure when thread ends or is interrupted
            try:
                page.quit()
            except:
                pass

    def process_sbd(self, page, grade, sbd):
        try:
            khoi_btn = page.ele(
                "xpath://html/body/div[2]/main/div/div/div[2]/div/form/div[1]/div[1]/button",
                timeout=5,
            )
            if not khoi_btn:
                khoi_btn = page.ele('xpath://button[@role="combobox"]', timeout=3)

            if not khoi_btn:
                return False

            if str(grade) not in khoi_btn.text:
                khoi_btn.click(by_js=True)
                time.sleep(0.5)

                opt_grade = page.ele(
                    f'xpath://*[@role="option" and contains(., "{grade}")]', timeout=5
                )
                if opt_grade:
                    opt_grade.click(by_js=True)
                    time.sleep(0.2)
                else:
                    page.run_js(
                        "document.dispatchEvent(new KeyboardEvent('keydown', {'key': 'Escape'}));"
                    )
                    return False

            sbd_field = page.ele("#sbd", timeout=5)
            if sbd_field:
                sbd_field.input(sbd, clear=True)
            else:
                return False

            submit_btn = page.ele('xpath://button[contains(., "Tra cứu")]') or page.ele(
                "xpath://html/body/div[2]/main/div/div/div[2]/div/form/button"
            )

            if submit_btn:
                submit_btn.click(by_js=True)
            else:
                return False

            res = page.listen.wait(timeout=8)
            if res and res.response.body:
                data_json = res.response.body

                if isinstance(data_json, dict) and data_json.get("success"):
                    self.extract_data(data_json["data"], grade, sbd)

                time.sleep(0.5)
                close_btn = page.ele(
                    'xpath://button[contains(@class, "absolute") or contains(., "Đóng")]',
                    timeout=2,
                )

                if close_btn:
                    close_btn.click(by_js=True)
                    time.sleep(0.5)
                else:
                    return False

                return True

            return False

        except Exception as e:
            return False

    def extract_data(self, data, grade, sbd):
        entry = {}
        entry["Grade"] = grade

        for f in self.cfg["fields"]:
            entry[f["column_name"]] = data.get(f["json_key"], "")

        scores = data.get("scores", [])
        for s in scores:
            subj_name = s.get("name")
            if subj_name in self.cfg["subjects"]:
                entry[subj_name] = s.get("total")

        with self.lock:
            self.results.append(entry)

    def run(self):
        todo = []
        for grade_str, bounds in self.cfg.get("grades", {}).items():
            start_sbd = bounds.get("start")
            end_sbd = bounds.get("end")
            if start_sbd and end_sbd:
                for sbd in range(start_sbd, end_sbd + 1):
                    todo.append((str(grade_str), sbd))

        last_failed_count = -1
        consecutive_same_fails = 0

        os.makedirs("./tmp_profiles", exist_ok=True)

        try:
            while todo:
                print(f"\n[*] Processing batch of {len(todo)} items...")
                pbar = tqdm(total=len(todo), unit="sbd")
                failed = self.scrape_batch(todo, pbar)
                pbar.close()

                # If Ctrl+C was pressed, abort the while loop
                if self.stop_event.is_set():
                    break

                if not failed:
                    print("\n[+] All items completed successfully.")
                    break

                if len(failed) == last_failed_count:
                    consecutive_same_fails += 1
                else:
                    consecutive_same_fails = 0

                if consecutive_same_fails >= 3:
                    print(f"\n[!] Failure count ({len(failed)}) stabilized. Stopping.")
                    break

                last_failed_count = len(failed)
                todo = failed
                print(f"[*] Retrying {len(todo)} failed items...")

        except KeyboardInterrupt:
            # 🛑 Catch the Ctrl+C Signal
            print("\n\n[!] Keyboard Interrupt detected! Signaling workers to stop...")
            self.stop_event.set()
            print("[*] Waiting for browsers to securely close. Please wait...")

            # Wait for all running threads to close their browsers
            for t in self.active_threads:
                if t.is_alive():
                    t.join()
            print("[+] Browsers closed successfully.")

        finally:
            # Regardless of success or force quit, save the data
            self.save()

    def save(self):
        if not self.results:
            print("[!] No data to save.")
            return

        df = pd.DataFrame(self.results)

        cols = (
            ["Grade"]
            + [f["column_name"] for f in self.cfg["fields"]]
            + self.cfg["subjects"]
        )

        final_cols = [c for c in cols if c in df.columns]
        df = df[final_cols]

        with pd.ExcelWriter(self.output_path, engine="openpyxl") as writer:
            unique_grades = sorted(df["Grade"].unique(), key=lambda x: int(x))

            for grade in unique_grades:
                sheet_name = f"Khối {grade}"
                grade_df = df[df["Grade"] == grade].copy()
                grade_df.drop(columns=["Grade"], inplace=True, errors="ignore")
                grade_df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"\n[***] Data securely saved to {self.output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TB Scraper Python Edition")
    parser.add_argument("-s", "--schema", required=True, help="Path to schema.json")
    parser.add_argument("-o", "--output", help="Output file path")
    parser.add_argument(
        "-w", "--workers", type=int, default=1, help="Number of concurrent workers"
    )

    args = parser.parse_args()

    scraper = Scraper(args.schema, args.output, args.workers)
    scraper.run()
