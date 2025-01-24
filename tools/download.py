import sys
from pathlib import Path

root = str(Path(__file__).resolve().parents[1])
sys.path.append(root)

from dataset.downloader import Downloader

if __name__ == '__main__':
    assets_path = 'dataset/assets/dj30.json'
    start_date = '2024-01-01'
    end_date = '2025-01-01'

    downloader = Downloader(assets_path=assets_path,
                            start_date=start_date,
                            end_date=end_date,
                            exp_path='dataset/workdir/dj30')

    try:
        downloader.run()
    except KeyboardInterrupt:
        sys.exit()
