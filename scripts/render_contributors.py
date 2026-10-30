"""Fill the contributors block in site/index.html from the GitHub API.

Run by the Pages workflow before the site is uploaded. Bots are skipped and the
block holds avatars only. The markup between the two markers is also committed,
so the page still shows people when previewed locally.

    GH_TOKEN=... python3 scripts/render_contributors.py [owner/repo]
"""

import html
import json
import re
import subprocess
import sys
from pathlib import Path

PAGE = Path(__file__).resolve().parent.parent / "site" / "index.html"
START, END = "<!-- contributors:start -->", "<!-- contributors:end -->"


def fetch(repo: str) -> list[dict]:
    out = subprocess.run(
        [
            "gh", "api", "--paginate", f"repos/{repo}/contributors",
            "--jq", '.[] | select(.type == "User") | {login, html_url, avatar_url}',
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    # --jq prints one compact JSON object per line, across all pages.
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def render(people: list[dict]) -> str:
    links = "\n".join(
        f'      <a href="{html.escape(p["html_url"])}" title="{html.escape(p["login"])}">'
        f'<img src="{html.escape(p["avatar_url"].split("?")[0])}?s=96" alt="{html.escape(p["login"])}" '
        f'width="48" height="48" loading="lazy" /></a>'
        for p in people
    )
    return f"{START}\n{links}\n      {END}"


def main() -> None:
    repo = sys.argv[1] if len(sys.argv) > 1 else "dmux/minilake"
    people = fetch(repo)
    if not people:
        sys.exit("no contributors returned; leaving the page untouched")
    page = PAGE.read_text()
    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.S)
    if not pattern.search(page):
        sys.exit(f"markers not found in {PAGE}")
    PAGE.write_text(pattern.sub(lambda _: render(people), page, count=1))
    print(f"rendered {len(people)} contributors")


if __name__ == "__main__":
    main()
