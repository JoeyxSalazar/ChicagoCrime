from playwright.sync_api import sync_playwright
import pandas as pd
import re
import time
from datetime import timedelta
from creds import password as PW
from creds import username as UN
from arrestdb import ArrestDB

prox = {
    "server":"zproxy.lum-superproxy.io:33335",
    "username": UN,
    "password": PW
}

DB_PATH = "output.sqlite"
db = ArrestDB(DB_PATH)


class Scrape:
    def __init__(self, dataframe):
        self.df = dataframe
        self.df["ARREST DATE"] = pd.to_datetime(
          self.df["ARREST DATE"],
          format="%m/%d/%Y %I:%M:%S %p",
          errors="coerce"
        )
    
    def print_lastmonth(self):
        cutoff = pd.Timestamp.now() - timedelta(days=30)
        recent = self.df[self.df["ARREST DATE"] >= cutoff]
        recent.to_csv("output.txt", index=False)
                

    def run(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, proxy=None)
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()

            # Go once and solve CAPTCHA once
            
            page.goto(
                "https://publicsearch1.chicagopolice.org/Arrests",
                wait_until="domcontentloaded"
            )
            #print("\nSolve CAPTCHA if prompted.")
            #input("Press ENTER once you're past it...")

            for idx, cb_number in self.df["CB_NO"].items():
                page.goto(
                    f"https://publicsearch1.chicagopolice.org/Arrests?CbNumber={cb_number}",
                    wait_until="domcontentloaded"
                )

                page.click("button[type='submit']")

                page.wait_for_selector("text=Details")

                with page.expect_navigation(url="**/Arrests/Details/**"):
                    page.get_by_role("link", name="Details").click()

                page.wait_for_selector("text=History")

                data = page.evaluate("""
                () => {
                  const dls = Array.from(document.querySelectorAll('dl.dl-horizontal')).slice(0, 2);
                  const out = {};
                  for (const dl of dls) {
                    const nodes = Array.from(dl.querySelectorAll('dt, dd'));
                    for (let i = 0; i < nodes.length; i++) {
                      if (nodes[i].tagName === 'DT') {
                        const key = nodes[i].textContent.trim();
                        const next = nodes[i].nextElementSibling;
                        if (next && next.tagName === 'DD') {
                          out[key] = next.textContent.trim();
                        }
                      }
                    }
                  }
                  return out;
                }
                """)

                # normalize whitespace
                data = { k: v.replace("\n", "").replace("\t", "").strip() for k, v in data.items() } 
                data = { k: re.sub(r" {2,}", " ", v).strip() for k, v in data.items() }

                self.df.at[idx, "NAME"] = data.get("NAME")
                self.df.at[idx, "AGE"] = data.get("AGE")
                self.df.at[idx, "ARREST LOCATION"] = data.get("ARREST LOCATION")
                db.upsert_row(self.df.loc[idx].to_dict(), status="OK")



            context.close()
            browser.close()

def setup():
    df = pd.read_csv('data/ChicagoArrests.csv')
    dom_abusers = df[df["CHARGES STATUTE"].str.contains("720 ILCS 5.0/12-3.2-A-1|720 ILCS 5.0/12-3.3-A-1|720 ILCS 5.0/12-3.4-A-1|720 ILCS 5.0/12-3.5-A-1|720 ILCS 5.0/12-3.8-A-1|720 ILCS 5.0/12-3.9-A-1", case=False, na=False)].copy()
    #Add new columns to expose perpetrator
    dom_abusers.insert(2, "NAME", None)
    dom_abusers.insert(3, "AGE", None)
    dom_abusers.insert(4, "ARREST LOCATION", None)
    DV = Scrape(dom_abusers)
    DV.run()
  
if __name__ == "__main__":
    setup()

