import io
import pathlib
import zipfile

import numpy as np
import pandas as pd
import requests

URL_FXDD = "https://tools.fxdd.com/tools/M1Data/{pair}.zip"
PATH_HIST = "./downloads/hist/{pair}/{year}.parquet"


def update_hist(pair: str):

    # ダウンロードURL
    url = URL_FXDD.format(pair=pair)
    print(f"ダウンロード : {url} ... ", end="")

    # ダウンロード
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception(f"ダウンロード失敗 : {url}")
    else:
        print("OK")

    # zipを解凍してhstを保存
    print("zipを解凍 ... ", end="")
    bytesio: io.BytesIO = None
    with zipfile.ZipFile(io.BytesIO(res.content), "r") as zip_data:
        for info in zip_data.infolist():
            if info.filename == f"{pair}.hst":
                bytesio = io.BytesIO(zip_data.read(info.filename))
                break
    if bytesio is None:
        raise Exception("zipの中にヒストリカルデータが見つからない")
    else:
        print("OK")

    # zip内のhst読む
    print("データ変換")
    ver = np.frombuffer(bytesio.read(148)[:4], "i4")
    if ver == 400:
        dtype = [("t", "u4"), ("o", "f8"), ("l", "f8"), ("h", "f8"), ("c", "f8"), ("v", "f8")]
        df = pd.DataFrame(np.frombuffer(bytesio.read(), dtype=dtype))
        df = df["t o h l c v".split()]
    elif ver == 401:
        dtype = [("t", "u8"), ("o", "f8"), ("h", "f8"), ("l", "f8"), ("c", "f8"), ("v", "i8"), ("s", "i4"), ("r", "i8")]
        df = pd.DataFrame(np.frombuffer(bytesio.read(), dtype=dtype).astype(dtype[:-2]))
    df = df.set_index(pd.to_datetime(df["t"], unit="s")).drop("t", axis=1)

    # 年ごとにparquetで保存
    t_year = df.index.year
    for year in range(t_year.min(), t_year.max() + 1):
        # フィルタ
        sub_df = df[df.index.year == year]
        # 保存
        file_path = pathlib.Path(PATH_HIST.format(pair=pair, year=year))
        print(f"保存 : {file_path} ... ", end="")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        sub_df.to_parquet(PATH_HIST.format(pair=pair, year=year))
        print("OK")


if __name__ == "__main__":
    update_hist("USDJPY")
