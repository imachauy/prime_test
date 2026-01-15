# 1. ベースイメージ
FROM python:3.11-slim

# 2. 作業ディレクトリ作成
WORKDIR /app

# 3. 必要ファイルをコピー
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# 4. アプリケーションコードをすべてコピー
COPY . .

# 5. 起動コマンド（FastAPIをuvicornで立ち上げ）
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]