import os
import sys
from pathlib import Path
import asyncio

root = str(Path(__file__).resolve().parents[1])
sys.path.append(root)

from dataset.downloader import Downloader

if __name__ == '__main__':
    assets_path = os.path.join(root, 'dataset', 'assets', 'dj30.json')
    start_date = '2024-01-01'
    end_date = '2025-01-01'

    downloader = Downloader(assets_path=assets_path,
                            start_date=start_date,
                            end_date=end_date,
                            workdir=os.path.join(root, 'workdir', 'dj30'))

    try:
        asyncio.get_event_loop().run_until_complete(downloader.run())
    except KeyboardInterrupt:
        sys.exit()
