"""
Generates a fullscreen slideshow of all published Superset dashboards
and opens it in the default browser.

Usage:
  python3 scripts/present.py              # 30s per slide (default)
  python3 scripts/present.py --interval 60
"""

import argparse
import json
import os
import sys
import tempfile
import webbrowser
import requests

SUPERSET = "http://localhost:8088"
USER, PASS = "admin", "admin"

# ── auth ───────────────────────────────────────────────────────────────────

session = requests.Session()

def login():
    r = session.post(f"{SUPERSET}/api/v1/security/login",
                     json={"username": USER, "password": PASS, "provider": "db"})
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_dashboards(token):
    r = session.get(
        f"{SUPERSET}/api/v1/dashboard/?q=(filters:!((col:published,opr:DashboardIs,val:!t)),page_size:50)",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return [
        {"title": d["dashboard_title"], "slug": d["slug"] or str(d["id"])}
        for d in r.json()["result"]
    ]

# ── HTML generation ────────────────────────────────────────────────────────

HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Crypto Analytics Presentation</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0f1117; color: #fff; font-family: 'Segoe UI', sans-serif; overflow: hidden; height: 100vh; }}

  #slide-container {{ position: relative; width: 100vw; height: 100vh; }}

  .slide {{ position: absolute; inset: 0; opacity: 0; transition: opacity 0.6s ease; pointer-events: none; }}
  .slide.active {{ opacity: 1; pointer-events: all; }}

  iframe {{ width: 100%; height: 100%; border: none; background: #fff; }}

  /* top bar */
  #topbar {{
    position: fixed; top: 0; left: 0; right: 0; z-index: 100;
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 20px;
    background: linear-gradient(to bottom, rgba(15,17,23,0.92) 0%, transparent 100%);
    pointer-events: none;
  }}
  #slide-title {{ font-size: 1.1rem; font-weight: 600; letter-spacing: 0.04em; text-shadow: 0 1px 4px rgba(0,0,0,0.8); }}
  #slide-counter {{ font-size: 0.85rem; opacity: 0.7; }}

  /* progress bar */
  #progress-track {{
    position: fixed; bottom: 0; left: 0; right: 0; height: 3px;
    background: rgba(255,255,255,0.12); z-index: 100;
  }}
  #progress-fill {{
    height: 100%; width: 0%;
    background: linear-gradient(90deg, #4f9cf9, #a78bfa);
    transition: width linear;
  }}

  /* navigation dots */
  #dots {{
    position: fixed; bottom: 14px; left: 50%; transform: translateX(-50%);
    display: flex; gap: 8px; z-index: 100;
  }}
  .dot {{
    width: 8px; height: 8px; border-radius: 50%;
    background: rgba(255,255,255,0.3); cursor: pointer;
    transition: background 0.3s, transform 0.3s;
    pointer-events: all;
  }}
  .dot.active {{ background: #fff; transform: scale(1.3); }}

  /* controls hint */
  #hint {{
    position: fixed; bottom: 36px; right: 20px;
    font-size: 0.72rem; opacity: 0.4; z-index: 100;
    pointer-events: none;
  }}

  /* pause overlay */
  #pause-badge {{
    position: fixed; top: 14px; right: 20px; z-index: 200;
    background: rgba(255,255,255,0.15); border-radius: 6px;
    padding: 4px 10px; font-size: 0.75rem; letter-spacing: 0.06em;
    display: none;
  }}
</style>
</head>
<body>

<div id="topbar">
  <span id="slide-title"></span>
  <span id="slide-counter"></span>
</div>

<div id="slide-container"></div>

<div id="progress-track"><div id="progress-fill"></div></div>
<div id="dots"></div>
<div id="hint">← → navigate &nbsp;|&nbsp; Space pause &nbsp;|&nbsp; F fullscreen</div>
<div id="pause-badge">⏸ PAUSED</div>

<script>
const SLIDES = {slides_json};
const INTERVAL = {interval_ms};

let current = 0;
let paused  = false;
let timer   = null;
let progTimer = null;

const container = document.getElementById('slide-container');
const titleEl   = document.getElementById('slide-title');
const counterEl = document.getElementById('slide-counter');
const fill      = document.getElementById('progress-fill');
const dotsEl    = document.getElementById('dots');
const badge     = document.getElementById('pause-badge');

// build slides + dots
SLIDES.forEach((s, i) => {{
  const div = document.createElement('div');
  div.className = 'slide';
  div.id = `slide-${{i}}`;
  div.innerHTML = `<iframe src="${{s.url}}" allowfullscreen></iframe>`;
  container.appendChild(div);

  const dot = document.createElement('div');
  dot.className = 'dot';
  dot.onclick = () => goTo(i);
  dotsEl.appendChild(dot);
}});

function goTo(idx) {{
  document.querySelectorAll('.slide').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.dot').forEach(el => el.classList.remove('active'));
  current = (idx + SLIDES.length) % SLIDES.length;
  document.getElementById(`slide-${{current}}`).classList.add('active');
  dotsEl.children[current].classList.add('active');
  titleEl.textContent = SLIDES[current].title;
  counterEl.textContent = `${{current + 1}} / ${{SLIDES.length}}`;
  startProgress();
}}

function startProgress() {{
  clearInterval(progTimer);
  fill.style.transition = 'none';
  fill.style.width = '0%';
  requestAnimationFrame(() => {{
    requestAnimationFrame(() => {{
      fill.style.transition = `width ${{INTERVAL}}ms linear`;
      fill.style.width = '100%';
    }});
  }});
}}

function scheduleNext() {{
  clearTimeout(timer);
  if (!paused) timer = setTimeout(() => {{ goTo(current + 1); scheduleNext(); }}, INTERVAL);
}}

function togglePause() {{
  paused = !paused;
  badge.style.display = paused ? 'block' : 'none';
  if (paused) {{
    clearTimeout(timer);
    clearInterval(progTimer);
    fill.style.transition = 'none';
  }} else {{
    startProgress();
    scheduleNext();
  }}
}}

document.addEventListener('keydown', e => {{
  if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {{ goTo(current + 1); scheduleNext(); }}
  else if (e.key === 'ArrowLeft'  || e.key === 'ArrowUp')  {{ goTo(current - 1); scheduleNext(); }}
  else if (e.key === ' ') {{ e.preventDefault(); togglePause(); }}
  else if (e.key === 'f' || e.key === 'F') {{ document.documentElement.requestFullscreen?.(); }}
}});

// start
goTo(0);
scheduleNext();
</script>
</body>
</html>
"""

def build_html(dashboards, interval_sec):
    slides = [
        {"title": d["title"], "url": f"{SUPERSET}/superset/dashboard/{d['slug']}/?standalone=1"}
        for d in dashboards
    ]
    return HTML.format(
        slides_json=json.dumps(slides, ensure_ascii=False),
        interval_ms=interval_sec * 1000,
    )

# ── main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=30, help="Seconds per slide (default 30)")
    args = parser.parse_args()

    print("Logging into Superset...")
    token = login()

    print("Fetching published dashboards...")
    dashboards = fetch_dashboards(token)
    if not dashboards:
        print("No published dashboards found.")
        sys.exit(1)
    for d in dashboards:
        print(f"  • {d['title']}  ({d['slug']})")

    html = build_html(dashboards, args.interval)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, encoding="utf-8"
    )
    tmp.write(html)
    tmp.close()

    print(f"\nOpening presentation ({args.interval}s per slide)...")
    print("Controls: ← → navigate  |  Space pause  |  F fullscreen")
    webbrowser.open(f"file://{tmp.name}")

if __name__ == "__main__":
    main()
