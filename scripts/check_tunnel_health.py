#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parents[1]
URL_FILE = BASE_DIR / 'current_tunnel_url.txt'


def fail(msg: str, code: int = 1) -> int:
    print(msg)
    return code


if not URL_FILE.exists():
    raise SystemExit(fail('CRIT missing current_tunnel_url.txt'))

url = URL_FILE.read_text(encoding='utf-8').strip()
if not url:
    raise SystemExit(fail('CRIT empty tunnel url'))

if not url.startswith('https://'):
    raise SystemExit(fail(f'CRIT invalid tunnel url: {url}'))

req = Request(url, headers={'User-Agent': 'beauty-vip-healthcheck/1.0'})
try:
    with urlopen(req, timeout=10) as resp:
        status = getattr(resp, 'status', None) or resp.getcode()
        final_url = getattr(resp, 'url', url)
        if status != 200:
            raise SystemExit(fail(f'CRIT status={status} url={final_url}'))
        print(f'OK status=200 url={final_url}')
except HTTPError as e:
    raise SystemExit(fail(f'CRIT status={e.code} url={url}'))
except URLError as e:
    raise SystemExit(fail(f'CRIT url_error={e.reason} url={url}'))
except Exception as e:
    raise SystemExit(fail(f'CRIT error={e} url={url}'))
