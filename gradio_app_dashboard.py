import gradio as gr
from datetime import datetime, timezone, timedelta
import uuid
import re
import os
import sys
import ast
import time
import random
import logging
import traceback
import numpy as np
import pandas as pd
from scipy.optimize import minimize
import clickhouse_connect
from pymongo import MongoClient, UpdateOne
from openai import OpenAI
from dotenv import load_dotenv

JST = timezone(timedelta(hours=9))
BKT_UPDATE_INTERVAL = timedelta(days=7)
DEFAULT_LLM_MODEL = "gpt-6-sol"

# ==========================================
# システムロガー設定 (JSTタイムスタンプ・即時フラッシュ対応)
# ==========================================
class JSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=JST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


logger = logging.getLogger("PRIME_SYSTEM")
logger.setLevel(logging.DEBUG)
logger.propagate = False

if not logger.handlers:
    _handler = FlushStreamHandler(sys.stdout)
    _handler.setLevel(logging.DEBUG)
    _formatter = JSTFormatter("[%(asctime)s JST] [%(levelname)s] [%(funcName)s:%(lineno)d] %(message)s")
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)


# ==========================================
# 0. データベース設定
# ==========================================
load_dotenv()
logger.info("環境変数 (.env) を読み込みました。")

MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    logger.warning("環境変数 MONGO_URL が設定されていません。")
else:
    logger.info("MongoDB クライアントを初期化します。")

mongo_client = MongoClient(MONGO_URL)
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
test_col = quiz_generator_db["test"]
logs_col = quiz_generator_db["logs"]
xapi_col = quiz_generator_db["xAPI"]
history_col = quiz_generator_db["history"]

# ClickHouse データベース選択設定
CLICKHOUSE_CONFIGS = {}
_i = 1
while True:
    _key = os.getenv(f"LTI_CONSUMER_KEY_{_i}")
    if not _key:
        break
    CLICKHOUSE_CONFIGS[_key] = {
        "host": os.getenv(f"BOOKROLL_DATABASE_HOST_{_i}"),
        "username": os.getenv(f"BOOKROLL_DATABASE_USER_{_i}"),
        "password": os.getenv(f"BOOKROLL_DATABASE_PASS_{_i}"),
        "db_name": os.getenv(f"BOOKROLL_DATABASE_NAME_{_i}", "saikyo_new")
    }
    logger.info(
        f"ClickHouse設定登録 [index={_i}]: school_id='{_key}', "
        f"host='{CLICKHOUSE_CONFIGS[_key]['host']}', db='{CLICKHOUSE_CONFIGS[_key]['db_name']}'"
    )
    _i += 1

if not CLICKHOUSE_CONFIGS:
    logger.warning("CLICKHOUSE_CONFIGS が1件も登録されませんでした。環境変数 LTI_CONSUMER_KEY_1 等を確認してください。")


def get_clickhouse_client_and_db(school_id):
    """school_idに対応するClickHouseクライアントとデータベース名を返す"""
    config = CLICKHOUSE_CONFIGS.get(school_id)
    if not config:
        logger.warning(f"school_id='{school_id}' に対応するClickHouse設定が CLICKHOUSE_CONFIGS に存在しません。")
        return None, None
    try:
        logger.debug(f"ClickHouseへ接続試行中: school_id='{school_id}', host='{config['host']}', db='{config['db_name']}'")
        client = clickhouse_connect.get_client(
            host=config["host"],
            username=config["username"],
            password=config["password"]
        )
        logger.debug(f"ClickHouse接続成功: school_id='{school_id}'")
        return client, config["db_name"]
    except Exception as e:
        logger.error(f"ClickHouse接続エラー (school_id='{school_id}'): {e}\n{traceback.format_exc()}")
        return None, None


# ==========================================
# LTIセッション取得ヘルパー (request.session['user'] 直接参照)
# ==========================================
def extract_lti_from_request(request: gr.Request = None, current_lti: dict = None):
    """
    request.session['user'] から直接LTI辞書を取得し、school_id / context_id を正規化して返す。
    """
    lti = {}
    if isinstance(current_lti, dict) and current_lti:
        lti.update(current_lti)

    if request is not None:
        try:
            session_user = request.session["user"]
            if session_user:
                lti.update(dict(session_user))
                logger.info(f"[Session] request.session['user'] からLTI情報を取得しました: {lti}")
        except Exception as e:
            logger.warning(f"[Session] request.session['user'] の取得に失敗しました ({e})")

    if not lti.get("user_id"):
        lti = {
            "user_id": "3539",
            "school_id": "C126210001533",
            "oauth_consumer_key": "C126210001533",
            "context_id": "799",
            "roles": "Learner",
            "context_title": "中学1年数学"
        }
        logger.warning(f"[Session] デフォルトのLTI情報を使用します: {lti}")

    # school_id と context_id のキー揺れ (oauth_consumer_key 等) を補完
    school_id = lti.get("school_id") or lti.get("oauth_consumer_key") or "unknown"
    context_id = lti.get("context_id") or lti.get("course_id") or "unknown"

    lti["user_id"] = str(lti["user_id"])
    lti["school_id"] = str(school_id)
    lti["context_id"] = str(context_id)
    return lti["user_id"], lti


# ==========================================
# xAPI マッピング定義 (TinCan Registry Verb URI 準拠)
# ==========================================
VERB_URI_MAP = {
    "started": "https://w3id.org/xapi/dod-isd/verbs/started",
    "selected": "http://id.tincanapi.com/verb/selected",
    "submitted": "http://activitystrea.ms/schema/1.0/submit",
    "enabled": "http://id.tincanapi.com/verb/enabled",
    "viewed": "http://id.tincanapi.com/verb/viewed",
    "updated": "http://activitystrea.ms/schema/1.0/update",
    "attempted": "http://adlnet.gov/expapi/verbs/attempted",
    "skipped": "http://id.tincanapi.com/verb/skipped"
}

XAPI_SPEC_TABLE = {
    "StartedSession": {
        "system": "PRIME_TEST",
        "verb_display": "started",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Others",
        "contextActivities3": "StartedSession"
    },
    "SelectedTest": {
        "system": "PRIME_TEST",
        "verb_display": "selected",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Select",
        "contextActivities3": "SelectedTest"
    },
    "StartedCreatingTest": {
        "system": "PRIME_TEST",
        "verb_display": "submitted",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Create",
        "contextActivities3": "StartedCreatingTest"
    },
    "PreparedTest": {
        "system": "PRIME_TEST",
        "verb_display": "enabled",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Work",
        "contextActivities3": "PreparedTest"
    },
    "StartedReviewTest": {
        "system": "PRIME_TEST",
        "verb_display": "viewed",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Work",
        "contextActivities3": "StartedReviewTest"
    },
    "ChangedAnswer": {
        "system": "PRIME",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Work",
        "contextActivities3": "ChangedAnswer"
    },
    "ShowedAnswer": {
        "system": "PRIME_TEST",
        "verb_display": "attempted",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Check",
        "contextActivities3": "ShowedAnswer"
    },
    "UpdatedPerQuestionFeedback_Understanding": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "UpdatedPerQuestionFeedback_Understanding"
    },
    "UpdatedPerQuestionFeedback_Rating": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "UpdatedPerQuestionFeedback_Rating"
    },
    "UpdatedPerQuestionFeedback_Difficulty": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "UpdatedPerQuestionFeedback_Difficulty"
    },
    "UpdatedPerQuestionFeedback_Isincurriculum": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "UpdatedPerQuestionFeedback_Isincurriculum"
    },
    "UpdatedPerQuestionFeedback_Relatedtowords": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "UpdatedPerQuestionFeedback_Relatedtowords"
    },
    "NavigatedPrev": {
        "system": "PRIME_TEST",
        "verb_display": "viewed",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Work",
        "contextActivities3": "NavigatedPrev"
    },
    "NavigatedNext": {
        "system": "PRIME_TEST",
        "verb_display": "viewed",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Work",
        "contextActivities3": "NavigatedNext"
    },
    "FinishedTest": {
        "system": "PRIME_TEST",
        "verb_display": "submitted",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Others",
        "contextActivities3": "FinishedTest"
    },
    "QuittedTest": {
        "system": "PRIME_TEST",
        "verb_display": "skipped",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Others",
        "contextActivities3": "QuittedTest"
    },
    "ChangeFinalRating": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "ChangeFinalRating"
    },
    "ChangeFeedback": {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Reflect",
        "contextActivities3": "ChangeFeedback"
    },
    "SubmitOverallFeedback": {
        "system": "PRIME_TEST",
        "verb_display": "submitted",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Others",
        "contextActivities3": "SubmitOverallFeedback"
    }
}

OPERATION_NAME_ALIASES = {
    "StartReviewTest": "StartedReviewTest",
    "ShowAnswer": "ShowedAnswer",
    "NavigatePrev": "NavigatedPrev",
    "NavigateNext": "NavigatedNext",
    "FinishTest": "FinishedTest"
}


def encode_feedback_value(field_type, raw_val):
    """ラジオボタン等の選択値をxAPI用の数値文字列に変換する"""
    if raw_val is None:
        return ""
    s = str(raw_val).strip()
    if not s:
        return ""

    m = re.match(r"^(\d+)", s)
    if m:
        return m.group(1)

    if field_type == "Understanding":
        mapping = {
            "すべて自力で解けた": "3",
            "解説を見てわかった": "2",
            "解説を見てもわからなかった": "1"
        }
        return mapping.get(s, s)

    if field_type == "Isincurriculum":
        mapping = {
            "含んでいたので、問題が解けなかった": "1",
            "含んでいたが、問題は解けた": "2",
            "含んでいなかった": "3"
        }
        return mapping.get(s, s)

    return s


# ==========================================
# LLM問題生成用プロンプトテンプレート群
# ==========================================
PROMPT_FREE = """あなたはプロの数学教師です。
[GRADE]向けの数学の問題を自由に1問作成してください。

【出力・フォーマットに関する注意事項】
- 新しい問題のみを結果として出力すること。
- 問題は以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
  例：$$\\int_{}^{} f(x)\\,dx = F(b) - F(a)$$
- mathjaxフォーマットの&は使わないこと。
- \\text フォーマットは使わないこと。
- <, >の2つの記号は、必ず "<\\," ">\\," という形で出力すること。
- $という記号は、フォーマットで与えられている2組の$$を除き使用しないこと。
- 以下のフォーマットで、XXXXに問題を挿入して、左揃えで答えること。

[問題] 
$$ \\begin{array}{l} XXXX \\end{array} $$"""

PROMPT_LOW = """あなたはプロの数学教師です。
以下の指定された「単元」と「キーワード」をすべて含んだ基本問題を1問作成してください。

【対象単元】
[SUBUNIT]

【必ず含めるキーワード】
[KEYWORDS]

【難易度と認知レベルの指定】
TIMSSの認知領域における「知ること（知識）」レベルをターゲットにしてください。
数学的な事実、概念、道具、手順を基にした知識に関することを確認する問題構成にしてください。

【既習範囲の制限】
以下の単元は「既習（すでに習った範囲）」として問題に使用して構いません。
[LEARNED_SUBUNITS]
※上記に記載されていない単元や概念は「未習」とし、絶対に問題やその解法に含めないでください。

【出力・フォーマットに関する注意事項】
- 新しい問題のみを結果として出力すること。
- 問題は以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
  例：$$\\int_{}^{} f(x)\\,dx = F(b) - F(a)$$
- mathjaxフォーマットの&は使わないこと。
- \\text フォーマットは使わないこと。
- <, >の2つの記号は、必ず "<\\," ">\\," という形で出力すること。
- $という記号は、フォーマットで与えられている2組の$$を除き使用しないこと。
- 以下のフォーマットで、XXXXに問題を挿入して、左揃えで答えること。
[問題] 
$$ \\begin{array}{l} XXXX \\end{array} $$"""

PROMPT_MIDDLE_HIGH = """あなたはプロの数学教師です。
以下の指定された複数の「単元」の知識を組み合わせ、かつ「キーワード」をすべて含んだ応用問題を1問作成してください。

【対象単元（すべて組み合わせること）】
[SUBUNIT]

【必ず含めるキーワード】
[KEYWORDS]

[DIFFICULTY_CONDITION]

【既習範囲の制限】
以下の単元は「既習（すでに習った範囲）」として問題に使用して構いません。
[LEARNED_SUBUNITS]
※上記に記載されていない単元や概念は「未習」とし、絶対に問題やその解法に含めないでください。

【出力・フォーマットに関する注意事項】
- 新しい問題のみを結果として出力すること。
- 問題は以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
  例：$$\\int_{}^{} f(x)\\,dx = F(b) - F(a)$$
- mathjaxフォーマットの&は使わないこと。
- \\text フォーマットは使わないこと.
- <, >の2つの記号は, 必ず "<\\," ">\\," という形で出力すること。
- $という記号は、フォーマットで与えられている2組の$$を除き使用しないこと。
- 以下のフォーマットで、XXXXに問題を挿入して、左揃えで答えること。
[問題] 
$$ \\begin{array}{l} XXXX \\end{array} $$"""

CONDITION_APPLY = """【難易度と認知レベルの指定】
TIMSSの認知領域における「応用すること（応用）」レベルをターゲットにしてください。
知識や概念的理解を問題場面に応用することを求める問題構成にしてください。"""

CONDITION_REASONING = """【難易度と認知レベルの指定】
TIMSSの認知領域における「推論を行うこと（推論）」レベルをターゲットにしてください。
見慣れない場面の問題や複雑な文脈の問題、あるいは多段階の問題を解くことを求める構成にしてください。"""


# ==========================================
# ヘルパー関数群
# ==========================================
def find_grade(context):
    grades_list = ["中学1年", "中学2年", "中学3年", "高校1年", "高校2年", "高校3年"]
    for grade in grades_list:
        if grade in str(context):
            return grade
    return "中学3年"


def normalize_page_no(p_val):
    """ページ番号の表記揺れ（bytes, 空白, 先頭ゼロ '062' -> '62', 小数点 '62.0' -> '62'）を正規化する"""
    if p_val is None:
        return ""
    if isinstance(p_val, bytes):
        s = p_val.decode("utf-8", errors="ignore")
    else:
        s = str(p_val)
    s = s.replace("\x00", "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        return str(int(s))
    return s


def parse_keywords_field(raw_kw):
    """文字列・リスト・カンマ区切り等のキーワードをリストに正規化する"""
    if not raw_kw:
        return []
    if isinstance(raw_kw, list):
        return [str(k).strip() for k in raw_kw if str(k).strip()]
    if isinstance(raw_kw, str):
        raw_kw = raw_kw.strip()
        if raw_kw.startswith("[") and raw_kw.endswith("]"):
            try:
                parsed = ast.literal_eval(raw_kw)
                if isinstance(parsed, list):
                    return [str(k).strip() for k in parsed if str(k).strip()]
            except Exception:
                pass
        return [k.strip() for k in re.split(r"[,、，]", raw_kw) if k.strip()]
    return []


def map_response_to_category_and_score(res_str):
    """
    ClickHouseおよびMongoDBの解答結果文字列を
    ('全問正解' | '一部正解' | '全問不正解', 1.0 | 0.5 | 0.0, 'green' | 'yellow' | 'red') に変換する
    """
    if res_str is None or (isinstance(res_str, float) and np.isnan(res_str)):
        return None, None, None
    res = str(res_str).strip()
    if not res:
        return None, None, None

    res_lower = res.lower()
    if (
        res == "全問正解"
        or "自力" in res
        or ("正解" in res and "不正解" not in res and "一部" not in res)
        or res_lower in ("true", "correct", "ok", "pass")
    ):
        return "全問正解", 1.0, "green"
    elif res == "一部正解" or "一部" in res or "見てわかった" in res or res_lower in ("partial", "half"):
        return "一部正解", 0.5, "yellow"
    elif (
        res == "全問不正解"
        or "まったく" in res
        or "わから" in res
        or "不正解" in res
        or res_lower in ("false", "incorrect", "ng", "fail")
    ):
        return "全問不正解", 0.0, "red"

    try:
        val = float(res)
        if val > 1.0:
            val = val / 100.0
        if val >= 0.9:
            return "全問正解", 1.0, "green"
        elif val >= 0.4:
            return "一部正解", 0.5, "yellow"
        else:
            return "全問不正解", 0.0, "red"
    except ValueError:
        return None, None, None


def normalize_tz(dt):
    """タイムスタンプをUTCのdatetimeオブジェクトに正規化する"""
    if not dt:
        return datetime.min.replace(tzinfo=timezone.utc)
    if isinstance(dt, dict) and "$date" in dt:
        dt = dt["$date"]
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    if isinstance(dt, datetime):
        if getattr(dt, "tzinfo", None) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    return datetime.min.replace(tzinfo=timezone.utc)


def get_latest_bkt_entry(doc):
    """
    question_bank のドキュメントから最新の履歴エントリを取得する。
    bkt_history が存在する場合は日付が最新の要素を返し、ない場合はトップレベル属性から構築する。
    """
    history = doc.get("bkt_history")
    if isinstance(history, list) and len(history) > 0:
        valid_entries = [h for h in history if isinstance(h, dict)]
        if valid_entries:
            valid_entries.sort(key=lambda h: normalize_tz(h.get("timestamp") or h.get("date")))
            return valid_entries[-1]

    def _extract_latest_scalar(val, default):
        if isinstance(val, list) and val:
            last_item = val[-1]
            if isinstance(last_item, dict):
                return float(last_item.get("value", default))
            try:
                return float(last_item)
            except (ValueError, TypeError):
                return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    diff_val = max(0.0, min(1.0, _extract_latest_scalar(doc.get("difficulty"), 0.5)))
    g_val = _extract_latest_scalar(doc.get("guess"), 0.25 - 0.20 * diff_val)
    s_val = _extract_latest_scalar(doc.get("slip"), 0.01 + 0.08 * diff_val)
    l0_val = _extract_latest_scalar(doc.get("L0"), 0.5)
    t_val = _extract_latest_scalar(doc.get("T"), 0.1)
    updated_at = doc.get("bkt_updated_at")

    return {
        "date": updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at or ""),
        "timestamp": updated_at,
        "difficulty": diff_val,
        "guess": g_val,
        "slip": s_val,
        "L0": l0_val,
        "T": t_val
    }


def is_bkt_recently_updated(doc, now_utc=None):
    """対象ドキュメントの最新BKTデータが過去1週間（7日）以内に記録されたものか判定する"""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    latest_entry = get_latest_bkt_entry(doc)
    ts = latest_entry.get("timestamp") or latest_entry.get("date") or doc.get("bkt_updated_at")
    if not ts:
        return False
    dt_utc = normalize_tz(ts)
    if dt_utc == datetime.min.replace(tzinfo=timezone.utc):
        return False
    return (now_utc - dt_utc) < BKT_UPDATE_INTERVAL


# ==========================================
# 0.5 コンテナ起動時の難易度・BKTパラメータ一括計算 (履歴保持 & 1週間以内スキップ対応)
# ==========================================
_BKT_STARTUP_DONE = False


def initialize_bkt_parameters_on_startup(mongo_db, force=False):
    global _BKT_STARTUP_DONE
    if _BKT_STARTUP_DONE and not force:
        logger.debug("[Startup] BKT初期計算は既に実行済みのためスキップします。")
        return
    _BKT_STARTUP_DONE = True

    startup_t0 = time.time()
    now_jst = datetime.now(JST)
    now_utc = now_jst.astimezone(timezone.utc)
    today_iso = now_jst.isoformat()

    logger.info("==================================================================")
    logger.info("[Startup] 難易度およびBKTパラメータの初期計算チェックを開始します。")
    logger.info("==================================================================")

    try:
        qb_docs = list(mongo_db["question_bank"].find({"contents_id": {"$exists": True}}))
    except Exception as e:
        logger.error(f"[Startup] MongoDB question_bank の取得に失敗しました: {e}\n{traceback.format_exc()}")
        return

    logger.info(f"[Startup] question_bank から {len(qb_docs)} 件のドキュメントを取得しました。")
    if not qb_docs:
        logger.warning("[Startup] question_bank にデータが存在しないため、初期計算をスキップします。")
        return

    docs_need_update = [doc for doc in qb_docs if not is_bkt_recently_updated(doc, now_utc)]
    recently_updated_count = len(qb_docs) - len(docs_need_update)

    if not force and len(docs_need_update) == 0:
        logger.info(
            f"[Startup] 全 {len(qb_docs)} 件の最新BKT・難易度データが過去1週間以内に記録されているため、"
            "再計算および更新をスキップします。"
        )
        return

    logger.info(
        f"[Startup] 更新判定結果: 1週間以内更新済み={recently_updated_count}件(スキップ対象), "
        f"更新対象={len(docs_need_update)}件"
    )

    match_key_to_doc_ids = {}
    match_key_to_unit_sub = {}
    match_key_to_prev_key = {}
    cid_page_no_to_match_key = {}

    orig_count = 0
    ai_gen_count = 0
    unknown_unit_count = 0

    for doc in qb_docs:
        doc_id = doc["_id"]
        c_id = str(doc.get("contents_id", "")).strip()
        p_no = normalize_page_no(doc.get("page", ""))
        n_no = str(doc.get("no", "")).strip()
        unit = str(doc.get("unit", "")).strip() or "Unknown"
        subunit = str(doc.get("subunit", "")).strip() or "Unknown"

        if unit == "Unknown" or subunit == "Unknown":
            unknown_unit_count += 1

        if not c_id or not p_no:
            continue

        if c_id == "ai_generated":
            m_key = f"{c_id}_{p_no}_{n_no}".lower()
            ai_gen_count += 1
            prev_q = doc.get("previous_quiz")
            if isinstance(prev_q, dict) and prev_q.get("contents_id") and prev_q.get("page"):
                prev_cid = str(prev_q.get("contents_id")).strip()
                prev_pno = normalize_page_no(prev_q.get("page"))
                match_key_to_prev_key[m_key] = f"{prev_cid}_{prev_pno}".lower()
        else:
            m_key = f"{c_id}_{p_no}".lower()
            orig_count += 1

        if force or not is_bkt_recently_updated(doc, now_utc):
            match_key_to_doc_ids.setdefault(m_key, []).append(doc_id)

        if m_key not in match_key_to_unit_sub or match_key_to_unit_sub[m_key] == ("Unknown", "Unknown"):
            match_key_to_unit_sub[m_key] = (unit, subunit)

        if n_no:
            cid_page_no_to_match_key[(c_id, p_no, n_no)] = m_key

    logger.info(
        f"[Startup] インデックス構築完了: 全ユニークmatch_key={len(match_key_to_unit_sub)}件, "
        f"今回更新対象のmatch_key={len(match_key_to_doc_ids)}件 "
        f"(通常問題={orig_count}件, AI生成問題={ai_gen_count}件, 単元未設定={unknown_unit_count}件)"
    )

    raw_logs = []

    # (A) ClickHouse から全ログ取得
    queried_ch_targets = set()
    for school_id in CLICKHOUSE_CONFIGS.keys():
        ch_client, db_name = get_clickhouse_client_and_db(school_id)
        if not ch_client or not db_name:
            continue
        target_sig = (CLICKHOUSE_CONFIGS[school_id]["host"], db_name)
        if target_sig in queried_ch_targets:
            continue
        queried_ch_targets.add(target_sig)

        try:
            ch_t0 = time.time()
            sql = f"""
            SELECT actor_name_id, contents_id, page_no, CAST(results_response AS String), timestamp
            FROM {db_name}.statements_target
            WHERE operation_name = 'ANSWER_QUIZ'
            """
            res = ch_client.query(sql)
            total_ch_rows = len(res.result_rows)
            matched_ch_rows = 0
            unmatched_key_rows = 0
            unmapped_resp_rows = 0

            for row in res.result_rows:
                actor = str(row[0]).strip() if row[0] else ""
                c_id = str(row[1]).strip() if row[1] else ""
                p_no = normalize_page_no(row[2])
                resp_str = str(row[3]) if row[3] is not None else ""
                ts = normalize_tz(row[4])

                if not actor or not c_id or not p_no:
                    continue

                m_key = f"{c_id}_{p_no}".lower()
                if m_key not in match_key_to_unit_sub:
                    unmatched_key_rows += 1
                    continue

                cat, score, _ = map_response_to_category_and_score(resp_str)
                if cat is None:
                    unmapped_resp_rows += 1
                    continue

                unit, subunit = match_key_to_unit_sub[m_key]
                raw_logs.append({
                    "actor_name_id": actor,
                    "match_key": m_key,
                    "results_response": cat,
                    "correct": score,
                    "timestamp": ts,
                    "unit": unit,
                    "subunit": subunit
                })
                matched_ch_rows += 1

            logger.info(
                f"[Startup] ClickHouse ({target_sig}) ログ取得完了 ({time.time() - ch_t0:.2f}秒): "
                f"総行数={total_ch_rows}, question_bank一致={matched_ch_rows}件, "
                f"キー不一致={unmatched_key_rows}件, レスポンス変換不可={unmapped_resp_rows}件"
            )
        except Exception as e:
            logger.error(f"[Startup] ClickHouse ログ取得エラー ({target_sig}): {e}\n{traceback.format_exc()}")

    # (B) MongoDB history コレクションから全ログ取得
    try:
        mg_t0 = time.time()
        history_docs = list(mongo_db["history"].find())
        matched_mg_rows = 0
        unmatched_mg_keys = 0
        unmapped_mg_rows = 0

        for h_doc in history_docs:
            actor = str(h_doc.get("user", "")).strip()
            c_id = str(h_doc.get("contents_id", "")).strip()
            p_no = normalize_page_no(h_doc.get("page", ""))
            n_no = str(h_doc.get("no", "")).strip()
            resp_str = h_doc.get("understanding", "")
            ts = normalize_tz(h_doc.get("timestamp"))

            if not actor or not c_id or not p_no:
                continue

            if c_id == "ai_generated" and n_no:
                m_key = cid_page_no_to_match_key.get((c_id, p_no, n_no), f"{c_id}_{p_no}_{n_no}".lower())
            else:
                m_key = f"{c_id}_{p_no}".lower()

            if m_key not in match_key_to_unit_sub:
                unmatched_mg_keys += 1
                continue

            cat, score, _ = map_response_to_category_and_score(resp_str)
            if cat is None:
                unmapped_mg_rows += 1
                continue

            unit, subunit = match_key_to_unit_sub[m_key]
            raw_logs.append({
                "actor_name_id": actor,
                "match_key": m_key,
                "results_response": cat,
                "correct": score,
                "timestamp": ts,
                "unit": unit,
                "subunit": subunit
            })
            matched_mg_rows += 1

        logger.info(
            f"[Startup] MongoDB history ログ取得完了 ({time.time() - mg_t0:.2f}秒): "
            f"総件数={len(history_docs)}, question_bank一致={matched_mg_rows}件, "
            f"キー不一致={unmatched_mg_keys}件, レスポンス変換不可={unmapped_mg_rows}件"
        )
    except Exception as e:
        logger.error(f"[Startup] MongoDB history ログ取得エラー: {e}\n{traceback.format_exc()}")

    # 2. 難易度 (difficulty) の計算
    diff_map = {}
    subunit_diff_list = {}

    if raw_logs:
        log_df = pd.DataFrame(raw_logs)
        log_df = log_df.sort_values("timestamp")

        first_attempts = log_df.drop_duplicates(subset=["actor_name_id", "match_key"], keep="first").copy()
        counts = pd.crosstab(first_attempts["match_key"], first_attempts["results_response"])

        for col in ["全問正解", "一部正解", "全問不正解"]:
            if col not in counts.columns:
                counts[col] = 0

        a = counts["全問正解"]
        b = counts["一部正解"]
        c = counts["全問不正解"]

        difficulty_series = 1.0 - ((a + 1.0 + 0.5 * (b + 1.0)) / (a + b + c + 3.0))
        diff_map = difficulty_series.to_dict()

        for m_key, d_val in diff_map.items():
            _, sub = match_key_to_unit_sub.get(m_key, ("Unknown", "Unknown"))
            if sub != "Unknown":
                subunit_diff_list.setdefault(sub, []).append(float(d_val))

        non_half_count = sum(1 for v in diff_map.values() if abs(v - 0.5) > 1e-6)
        diff_vals = list(diff_map.values())
        logger.info(
            f"[Startup] 難易度(difficulty)計算完了: 対象問題数={len(diff_map)}件 "
            f"(うち0.5以外の問題={non_half_count}件, 初回解答ログ数={len(first_attempts)}件, "
            f"min={min(diff_vals):.3f}, mean={np.mean(diff_vals):.3f}, max={max(diff_vals):.3f})"
        )
    else:
        log_df = pd.DataFrame(
            columns=["actor_name_id", "match_key", "results_response", "correct", "timestamp", "unit", "subunit"]
        )
        logger.warning("[Startup] 有効な解答ログが0件のため、全問題にデフォルト難易度(0.5)を適用します。")

    subunit_mean_diff = {sub: float(np.mean(vals)) for sub, vals in subunit_diff_list.items() if vals}

    # 3. unit / subunit ごとの BKT フィッティング (L0, T, guess, slip)
    subunit_params = {}
    item_bkt_params = {}

    if not log_df.empty:
        log_df = log_df.sort_values(by=["actor_name_id", "timestamp"])
        grouped = log_df.groupby(["unit", "subunit"])
        logger.info(f"[Startup] 全 {len(grouped)} 個の (unit, subunit) グループについてBKT最適化を開始します。")

        converged_count = 0
        default_applied_count = 0
        unconverged_count = 0

        for (unit, subunit), group_df in grouped:
            if unit == "Unknown" and subunit == "Unknown":
                continue

            users = group_df["actor_name_id"].unique()
            items = group_df["match_key"].unique()
            num_items = len(items)

            if len(group_df) < 5 or num_items == 0:
                subunit_params[(unit, subunit)] = (0.5, 0.1)
                for item in items:
                    raw_diff = diff_map.get(item, subunit_mean_diff.get(subunit, 0.5))
                    diff_val = max(0.0, min(1.0, float(raw_diff)))
                    item_bkt_params[item] = (0.25 - 0.20 * diff_val, 0.01 + 0.08 * diff_val)
                default_applied_count += 1
                continue

            item_to_idx = {item: i for i, item in enumerate(items)}
            initial_G = []
            initial_S = []
            for item in items:
                raw_diff = diff_map.get(item, subunit_mean_diff.get(subunit, 0.5))
                diff_val = max(0.0, min(1.0, float(raw_diff)))
                initial_G.append(0.25 - 0.20 * diff_val)
                initial_S.append(0.01 + 0.08 * diff_val)

            fast_user_data = []
            for u in users:
                user_logs = group_df[group_df["actor_name_id"] == u]
                log_list = [
                    (item_to_idx[item], correct)
                    for item, correct in zip(user_logs["match_key"], user_logs["correct"])
                ]
                fast_user_data.append(log_list)

            def objective_function(params):
                L0 = params[0]
                T = params[1]
                G = params[2:2 + num_items]
                S = params[2 + num_items:]
                total_squared_error = 0.0

                for log_list in fast_user_data:
                    L = L0
                    for idx, actual_correct in log_list:
                        g_i = G[idx]
                        s_i = S[idx]
                        p_correct = L * (1.0 - s_i) + (1.0 - L) * g_i
                        total_squared_error += (p_correct - actual_correct) ** 2

                        if actual_correct >= 0.5:
                            p_L_obs = (L * (1.0 - s_i)) / p_correct if p_correct > 0 else 0.0
                        else:
                            p_incorrect = L * s_i + (1.0 - L) * (1.0 - g_i)
                            p_L_obs = (L * s_i) / p_incorrect if p_incorrect > 0 else 0.0

                        L = p_L_obs + (1.0 - p_L_obs) * T
                return total_squared_error

            initial_params = [0.5, 0.1] + initial_G + initial_S
            bounds = (
                [(0.001, 0.999), (0.001, 0.999)]
                + [(0.001, 0.300)] * num_items
                + [(0.001, 0.100)] * num_items
            )

            try:
                res = minimize(
                    objective_function,
                    initial_params,
                    method="L-BFGS-B",
                    bounds=bounds,
                    options={"ftol": 1e-3, "disp": False}
                )
                best_L0 = float(res.x[0])
                best_T = float(res.x[1])
                best_G = res.x[2:2 + num_items]
                best_S = res.x[2 + num_items:]

                subunit_params[(unit, subunit)] = (best_L0, best_T)
                for i, item in enumerate(items):
                    item_bkt_params[item] = (float(best_G[i]), float(best_S[i]))

                if res.success:
                    converged_count += 1
                else:
                    unconverged_count += 1
            except Exception as e:
                logger.error(f"[Startup] BKT最適化例外 ({unit} - {subunit}): {e}\n{traceback.format_exc()}")
                subunit_params[(unit, subunit)] = (0.5, 0.1)
                for i, item in enumerate(items):
                    item_bkt_params[item] = (float(initial_G[i]), float(initial_S[i]))

        logger.info(
            f"[Startup] 単元別BKT最適化サマリ: 収束成功={converged_count}件, "
            f"未収束={unconverged_count}件, データ不足による初期値適用={default_applied_count}件"
        )

    # 4. 更新対象のレコードにのみ、日付付きの bkt_history 追加および最新値の更新を実行
    bulk_ops = []
    for m_key, doc_ids in match_key_to_doc_ids.items():
        unit, subunit = match_key_to_unit_sub.get(m_key, ("Unknown", "Unknown"))

        if m_key in diff_map:
            diff_val = float(diff_map[m_key])
        elif m_key in match_key_to_prev_key and match_key_to_prev_key[m_key] in diff_map:
            diff_val = float(diff_map[match_key_to_prev_key[m_key]])
        elif subunit in subunit_mean_diff:
            diff_val = float(subunit_mean_diff[subunit])
        else:
            diff_val = 0.5

        diff_val = max(0.0, min(1.0, diff_val))
        default_g = 0.25 - (0.20 * diff_val)
        default_s = 0.01 + (0.08 * diff_val)

        if m_key in item_bkt_params:
            g_val, s_val = item_bkt_params[m_key]
        elif m_key in match_key_to_prev_key and match_key_to_prev_key[m_key] in item_bkt_params:
            g_val, s_val = item_bkt_params[match_key_to_prev_key[m_key]]
        else:
            g_val, s_val = default_g, default_s

        l0_val, t_val = subunit_params.get((unit, subunit), (0.5, 0.1))

        rounded_diff = round(diff_val, 6)
        rounded_g = round(g_val, 6)
        rounded_s = round(s_val, 6)
        rounded_l0 = round(l0_val, 6)
        rounded_t = round(t_val, 6)

        history_entry = {
            "date": today_iso,
            "timestamp": now_jst,
            "difficulty": rounded_diff,
            "guess": rounded_g,
            "slip": rounded_s,
            "L0": rounded_l0,
            "T": rounded_t
        }

        for doc_id in doc_ids:
            bulk_ops.append(
                UpdateOne(
                    {"_id": doc_id},
                    {
                        "$set": {
                            "difficulty": rounded_diff,
                            "guess": rounded_g,
                            "slip": rounded_s,
                            "L0": rounded_l0,
                            "T": rounded_t,
                            "bkt_updated_at": now_jst
                        },
                        "$push": {
                            "bkt_history": history_entry
                        }
                    }
                )
            )

    if bulk_ops:
        try:
            bw_res = mongo_db["question_bank"].bulk_write(bulk_ops, ordered=False)
            logger.info(
                f"[Startup] question_bank 履歴追記・更新完了: 対象={len(bulk_ops)}件, "
                f"matched={bw_res.matched_count}件, modified={bw_res.modified_count}件 "
                f"(総所要時間: {time.time() - startup_t0:.2f}秒)"
            )
        except Exception as e:
            logger.error(f"[Startup] question_bank bulk_write エラー: {e}\n{traceback.format_exc()}")


# ==========================================
# ログ & xAPI 保存関数 (ADL xAPI 構造化表現・TinCan Registry Verb 準拠)
# ==========================================
def build_xapi_extensions(operation_name, session_id, value=None, extra_context=None):
    """各 operation_name の仕様に合わせて情報を損なわない extension ペイロードを組み立てる"""
    ext = {"session_id": str(session_id)}
    extra = extra_context if isinstance(extra_context, dict) else {}

    if operation_name == "StartedSession":
        if value is not None:
            ext["raw_value"] = value

    elif operation_name == "SelectedTest":
        if isinstance(value, dict):
            ext["test_id"] = str(value.get("test_id", ""))
            ext["test_name"] = str(value.get("test_name", ""))
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["test_id"] = str(extra.get("test_id", value or ""))
            ext["test_name"] = str(extra.get("test_name", ""))

    elif operation_name == "StartedCreatingTest":
        ext["review_type"] = "creating"
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v

    elif operation_name == "PreparedTest":
        ext["question_type"] = "creating"
        if isinstance(value, dict):
            ext["questions"] = value.get("questions", value)
            if "test_id" in value:
                ext["test_id"] = str(value["test_id"])
        else:
            ext["questions"] = {}

    elif operation_name == "StartedReviewTest":
        if isinstance(value, dict):
            ext["test_id"] = str(value.get("test_id", ""))
            if "test_name" in value:
                ext["test_name"] = str(value["test_name"])
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["test_id"] = str(value or "")
        ext["review_type"] = "reviewing"

    elif operation_name in ("ChangedAnswer", "ShowedAnswer"):
        if isinstance(value, dict):
            ext["question_no"] = str(value.get("question_no", "1"))
            ext["student_answer"] = str(value.get("student_answer", ""))
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["question_no"] = str(value if value is not None else "1")
            ext["student_answer"] = str(extra.get("student_answer", ""))

    elif operation_name.startswith("UpdatedPerQuestionFeedback_"):
        fb_type = operation_name.split("_", 1)[1]
        if fb_type == "Relatedtowords":
            ext["categories"] = "Relatedtowords"
            ext["category_description"] = "理解できた項目にチェックをつけよう"
            if isinstance(value, dict):
                ext["question_no"] = str(value.get("question_no", "1"))
                ext["value"] = value.get("value", {})
                if "selected_keywords" in value:
                    ext["selected_keywords"] = value["selected_keywords"]
            else:
                ext["value"] = value
        else:
            if isinstance(value, dict):
                ext["question_no"] = str(value.get("question_no", "1"))
                ext["type"] = fb_type
                ext["value"] = str(value.get("value", ""))
                if "raw_label" in value:
                    ext["raw_label"] = value["raw_label"]
                if "selections" in value:
                    ext["selections"] = value["selections"]
            else:
                ext["question_no"] = str(extra.get("question_no", "1"))
                ext["type"] = fb_type
                ext["value"] = encode_feedback_value(fb_type, value)
                ext["raw_label"] = value

    elif operation_name in ("NavigatedPrev", "NavigatedNext"):
        if isinstance(value, dict):
            ext["question_no"] = str(value.get("question_no", "1"))
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["question_no"] = str(value if value is not None else "1")

    elif operation_name == "FinishedTest":
        if isinstance(value, dict):
            ext["test_title"] = str(value.get("test_title", ""))
            ext["test_page"] = str(value.get("test_page", ""))
            ext["test_no"] = str(value.get("test_no", "4"))
            ext["quiz_fb"] = value.get("quiz_fb", {})
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["test_title"] = str(extra.get("test_title", ""))
            ext["test_page"] = str(extra.get("test_page", ""))
            ext["test_no"] = "4"
            ext["quiz_fb"] = {}

    elif operation_name == "QuittedTest":
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        elif value is not None:
            ext["question_no"] = str(value)

    elif operation_name == "ChangeFinalRating":
        ext["type"] = "Rating"
        if isinstance(value, dict):
            ext["value"] = str(value.get("value", ""))
            if "raw_label" in value:
                ext["raw_label"] = value["raw_label"]
        else:
            ext["value"] = encode_feedback_value("Rating", value)
            ext["raw_label"] = value

    elif operation_name == "ChangeFeedback":
        ext["type"] = "Report"
        if isinstance(value, dict):
            ext["value"] = str(value.get("value", ""))
        else:
            ext["value"] = str(value if value is not None else "")

    elif operation_name == "SubmitOverallFeedback":
        ext["type"] = "Rating"
        if isinstance(value, dict):
            raw_rating = value.get("rating", value.get("value", ""))
            ext["value"] = encode_feedback_value("Rating", raw_rating)
            ext["raw_rating"] = raw_rating
            ext["comment"] = value.get("comment", "")
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        else:
            ext["value"] = encode_feedback_value("Rating", value)
            ext["raw_rating"] = value

    else:
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in ext:
                    ext[k] = v
        elif value is not None:
            ext["value"] = value

    return ext


def handle_logs(user_id, operationname, session_id, lti, value=None, extra_context=None):
    """
    logs コレクションへの記録と同時に、ご指定のxAPI構造化表現（TinCan Registry Verb URI準拠）で
    quiz_generator_db["xAPI"] コレクションへ保存する。
    """
    if isinstance(lti, dict):
        school_id = lti.get("school_id") or lti.get("oauth_consumer_key") or "unknown"
        course_id = str(lti.get("context_id") or lti.get("course_id") or "unknown")
        roles = lti.get("roles", "")
        user_str = str(user_id or lti.get("user_id") or "3539")
    else:
        school_id, course_id, roles = "unknown", "unknown", ""
        user_str = str(user_id or "3539")

    now_jst = datetime.now(JST)
    canonical_op = OPERATION_NAME_ALIASES.get(operationname, operationname)

    # 1. 従来の logs コレクションへの保存
    try:
        logs_col.insert_one({
            "school_id": school_id,
            "course_id": course_id,
            "user": user_str,
            "user_role": roles,
            "session_id": session_id,
            "timestamp": now_jst,
            "operationname": canonical_op,
            "value": value
        })
    except Exception as e:
        logger.error(f"[UserActionLog] logs コレクションへの保存エラー: {e}\n{traceback.format_exc()}")

    # 2. xAPI コレクションへの保存 (ご指定の構造化表現)
    spec = XAPI_SPEC_TABLE.get(canonical_op, {
        "system": "PRIME_TEST",
        "verb_display": "updated",
        "object_name": "PRIME_TEST",
        "object_description": "数学テスト問題生成機能",
        "contextActivities1": "PRIME_TEST",
        "contextActivities2": "Others",
        "contextActivities3": canonical_op
    })

    verb_display = spec["verb_display"]
    verb_uri = VERB_URI_MAP.get(verb_display, f"http://id.tincanapi.com/verb/{verb_display}")

    object_extensions = build_xapi_extensions(canonical_op, session_id, value=value, extra_context=extra_context)
    if school_id and school_id != "unknown":
        object_extensions.setdefault("school_id", school_id)
    if course_id and course_id != "unknown":
        object_extensions.setdefault("course_id", course_id)
    if roles:
        object_extensions.setdefault("user_role", roles)

    xapi_doc = {
        "actor": {
            "account": {
                "homePage": "https://prime.example.com",
                "name": user_str
            }
        },
        "verb": {
            "id": verb_uri,
            "display": {
                "en-US": verb_display
            }
        },
        "object": {
            "id": f"https://w3id.org/xapi/PRIME/activities/{canonical_op}",
            "definition": {
                "name": {
                    "en-US": spec["object_name"]
                },
                "description": {
                    "en-US": spec["object_description"]
                },
                "extensions": {
                    "https://w3id.org/xapi/PRIME/extension": object_extensions
                }
            }
        },
        "context": {
            "contextActivities": {
                "category": [
                    {
                        "id": f"https://w3id.org/xapi/PRIME/context/{spec['contextActivities1']}"
                    },
                    {
                        "id": f"https://w3id.org/xapi/PRIME/context/{spec['contextActivities2']}"
                    },
                    {
                        "id": f"https://w3id.org/xapi/PRIME/context/{spec['contextActivities3']}"
                    }
                ]
            }
        },
        "timestamp": now_jst.isoformat()
    }

    try:
        xapi_col.insert_one(xapi_doc)
        logger.info(
            f"[xAPI Log] op='{canonical_op}' (verb='{verb_display}', uri='{verb_uri}'), "
            f"user='{user_str}', school='{school_id}', course='{course_id}', session='{session_id}'"
        )
    except Exception as e:
        logger.error(f"[xAPI Log] xAPI コレクションへの保存エラー: {e}\n{traceback.format_exc()}")


# ==========================================
# 1. リアルタイム問題生成コアロジック (最新BKT参照 & gpt-6-sol 対応)
# ==========================================
def run_realtime_student_generation(
    user_id,
    school_id,
    context_id,
    session_id,
    mongo_db,
    openai_client,
    model_name=DEFAULT_LLM_MODEL,
    grade="中学3年",
    lti_state=None
):
    initialize_bkt_parameters_on_startup(mongo_db)

    gen_t0 = time.time()
    user_str = str(user_id)
    course_id = str(context_id)

    logger.info("------------------------------------------------------------------")
    logger.info(
        f"[RealtimeGen] 問題生成開始: user='{user_str}', school_id='{school_id}', "
        f"course_id='{course_id}', session_id='{session_id}', model='{model_name}', grade='{grade}'"
    )

    try:
        test_count = mongo_db["test"].count_documents({"user": user_str})
        test_no_str = f"{test_count + 1:05d}"

        user_existing_questions = mongo_db["question_bank"].find({"user": user_str})
        max_no = 0
        for u_q in user_existing_questions:
            try:
                q_no = int(u_q.get("no", 0))
                if q_no > max_no:
                    max_no = q_no
            except (ValueError, TypeError):
                pass
        base_no = max_no + 1
        logger.info(f"[RealtimeGen] 採番情報: test_id='{test_no_str}', 開始問題番号 base_no={base_no}")
    except Exception as e:
        logger.error(f"[RealtimeGen] 採番処理中にMongoDBエラーが発生しました: {e}\n{traceback.format_exc()}")
        return False, f"データベースエラーが発生しました: {e}"

    # -----------------------------------------------------------
    # 0. question_bank から単元・最新の難易度・BKTパラメータを毎回取得
    # -----------------------------------------------------------
    master_questions = list(mongo_db["question_bank"].find({
        "unit": {"$nin": ["", None]},
        "subunit": {"$nin": ["", None]},
        "contents_id": {"$exists": True}
    }))
    logger.info(f"[RealtimeGen] 単元情報付き question_bank レコード読込: {len(master_questions)}件")

    knowledge_map = []
    unit_lookup = {}
    qb_by_cid_page = {}      # (contents_id, page) -> doc
    qb_by_cid_page_no = {}   # (contents_id, page, no) -> doc
    subunit_bkt_meta = {}    # subunit -> {"L0": float, "T": float, "timestamp": datetime}
    subunit_keywords = {}

    for q in master_questions:
        c_id = str(q.get("contents_id", "")).strip()
        p_no = normalize_page_no(q.get("page", ""))
        n_no = str(q.get("no", "")).strip()
        unit = str(q.get("unit", "")).strip()
        subunit = str(q.get("subunit", "")).strip()

        if not subunit:
            continue

        unit_lookup[subunit] = unit

        latest_bkt = get_latest_bkt_entry(q)
        entry_ts = normalize_tz(latest_bkt.get("timestamp") or latest_bkt.get("date"))
        if subunit not in subunit_bkt_meta or entry_ts >= subunit_bkt_meta[subunit]["timestamp"]:
            subunit_bkt_meta[subunit] = {
                "L0": float(latest_bkt["L0"]),
                "T": float(latest_bkt["T"]),
                "timestamp": entry_ts
            }

        kw_raw = q.get("keyword") or q.get("keywords")
        if kw_raw:
            subunit_keywords.setdefault(subunit, set()).update(parse_keywords_field(kw_raw))

        if c_id and p_no:
            qb_by_cid_page[(c_id, p_no)] = q
            if n_no:
                qb_by_cid_page_no[(c_id, p_no, n_no)] = q

        if c_id != "ai_generated":
            k_node = {
                "contents_id": c_id,
                "page": p_no,
                "answer_contents_id": q.get("answer_id"),
                "answer_page_start": int(q.get("answer_page_start", 0)) if str(q.get("answer_page_start", "")).isdigit() else 0,
                "answer_page_end": int(q.get("answer_page_end", 0)) if str(q.get("answer_page_end", "")).isdigit() else 0,
                "unit": unit,
                "subunit": subunit
            }
            knowledge_map.append(k_node)

    # -----------------------------------------------------------
    # 1. ClickHouse (元の問題の履歴 ＆ マーカーのカウント)
    # -----------------------------------------------------------
    all_history = []
    clickhouse_client, db_name = get_clickhouse_client_and_db(school_id)
    marker_pages_count = 0

    if clickhouse_client and db_name:
        try:
            sql = f"""
            SELECT contents_id, page_no, CAST(results_response AS String), timestamp
            FROM {db_name}.statements_target
            WHERE actor_name_id = {{user:String}}
              AND operation_name = 'ANSWER_QUIZ'
            ORDER BY timestamp ASC
            """
            res = clickhouse_client.query(sql, {"user": user_str})
            for row in res.result_rows:
                c_id = str(row[0]).strip() if row[0] else ""
                p_no = normalize_page_no(row[1])
                q_key = f"{c_id}_{p_no}"
                all_history.append({
                    "type": "original",
                    "q_key": q_key,
                    "c_id": c_id,
                    "p_no": p_no,
                    "n_no": "",
                    "result": str(row[2]) if row[2] else "",
                    "timestamp": row[3]
                })
            logger.info(f"[RealtimeGen] ClickHouse ANSWER_QUIZ 取得完了: user='{user_str}', 件数={len(res.result_rows)}件")
        except Exception as e:
            logger.error(f"[RealtimeGen] ClickHouse ANSWER_QUIZ 取得失敗: {e}\n{traceback.format_exc()}")

        course_contents_ids = set()
        try:
            for doc in exercise_col.find({"course_id": course_id}, {"contents_id": 1}):
                c_id = doc.get("contents_id")
                if c_id and c_id != "ai_generated":
                    course_contents_ids.add(c_id)
        except Exception as e:
            logger.error(f"[RealtimeGen] コース別 contents_id 抽出エラー: {e}")

        if course_contents_ids:
            try:
                sql_marker_count = f"""
                SELECT count()
                FROM (
                    SELECT contents_id, page_no
                    FROM {db_name}.statements_target
                    WHERE actor_name_id = {{user:String}}
                      AND contents_id IN {{contents_list:Array(String)}}
                      AND operation_name = 'ADD_MARKER'
                    GROUP BY contents_id, page_no
                )
                """
                res_marker = clickhouse_client.query(
                    sql_marker_count,
                    {"user": user_str, "contents_list": list(course_contents_ids)}
                )
                if res_marker.result_rows:
                    marker_pages_count = int(res_marker.result_rows[0][0])
                logger.info(f"[RealtimeGen] ClickHouse ADD_MARKER ページ数カウント: {marker_pages_count}ページ")
            except Exception as e:
                logger.error(f"[RealtimeGen] ClickHouse ADD_MARKER カウント取得失敗: {e}\n{traceback.format_exc()}")

    # -----------------------------------------------------------
    # 2. MongoDB (元の問題・復習問題の履歴)
    # -----------------------------------------------------------
    try:
        if school_id and school_id != "unknown":
            docs = list(history_col.find({"school_id": school_id, "user": user_str}))
        else:
            docs = list(history_col.find({"user": user_str}))
        for doc in docs:
            c_id = str(doc.get("contents_id", "")).strip()
            p_no = normalize_page_no(doc.get("page", ""))
            n_no = str(doc.get("no", "")).strip()
            q_type = "review" if c_id == "ai_generated" else "original"
            q_key = f"{c_id}_{p_no}_{n_no}"

            all_history.append({
                "type": q_type,
                "q_key": q_key,
                "c_id": c_id,
                "p_no": p_no,
                "n_no": n_no,
                "result": doc.get("understanding", ""),
                "timestamp": doc.get("timestamp")
            })
        logger.info(f"[RealtimeGen] MongoDB history 取得完了: user='{user_str}', 件数={len(docs)}件 (合計履歴={len(all_history)}件)")
    except Exception as e:
        logger.error(f"[RealtimeGen] MongoDB history 取得失敗: {e}\n{traceback.format_exc()}")

    try:
        all_history.sort(key=lambda x: normalize_tz(x["timestamp"]))
    except Exception as e:
        logger.warning(f"[RealtimeGen] all_history のタイムスタンプソート中に警告: {e}")

    # -----------------------------------------------------------
    # 3. 最新のパラメータ (bkt_historyの最新値) を用いたBKT計算
    # -----------------------------------------------------------
    latest_orig_results = {}
    latest_rev_results = {}

    user_bkt_dict = {}
    learned_subunits = set()

    unmapped_status_count = 0
    unmatched_qb_count = 0
    bkt_update_count = 0

    for x in all_history:
        res = str(x.get("result", ""))
        q_type = x["type"]
        q_key = x.get("q_key", "")
        c_id = x.get("c_id", "")
        p_no = x.get("p_no", "")
        n_no = x.get("n_no", "")

        _, actual_correct, status = map_response_to_category_and_score(res)
        if status is None or actual_correct is None:
            unmapped_status_count += 1
            continue

        if q_type == "original":
            latest_orig_results[q_key] = status
        else:
            latest_rev_results[q_key] = status

        matched_q = None
        if n_no and (c_id, p_no, n_no) in qb_by_cid_page_no:
            matched_q = qb_by_cid_page_no[(c_id, p_no, n_no)]
        elif (c_id, p_no) in qb_by_cid_page:
            matched_q = qb_by_cid_page[(c_id, p_no)]

        if not matched_q:
            unmatched_qb_count += 1
            continue

        subunit = str(matched_q.get("subunit", "")).strip()
        if not subunit:
            unmatched_qb_count += 1
            continue

        learned_subunits.add(subunit)

        latest_q_params = get_latest_bkt_entry(matched_q)
        meta = subunit_bkt_meta.get(subunit, {"L0": latest_q_params["L0"], "T": latest_q_params["T"]})
        L0_sub = float(meta["L0"])
        T_sub = float(meta["T"])
        diff_val = float(latest_q_params["difficulty"])
        g_i = float(latest_q_params["guess"])
        s_i = float(latest_q_params["slip"])

        L_prev = user_bkt_dict.get(subunit, L0_sub)

        if actual_correct >= 0.5:
            p_correct = L_prev * (1.0 - s_i) + (1.0 - L_prev) * g_i
            p_L_obs = (L_prev * (1.0 - s_i)) / p_correct if p_correct > 0 else 0.0
        else:
            p_incorrect = L_prev * s_i + (1.0 - L_prev) * (1.0 - g_i)
            p_L_obs = (L_prev * s_i) / p_incorrect if p_incorrect > 0 else 0.0

        L_new = p_L_obs + (1.0 - p_L_obs) * T_sub
        user_bkt_dict[subunit] = L_new
        bkt_update_count += 1

        logger.debug(
            f"[RealtimeGen][BKT更新] subunit='{subunit}', q_key='{q_key}', score={actual_correct}, "
            f"diff={diff_val:.3f}, g={g_i:.3f}, s={s_i:.3f}, L: {L_prev:.4f} -> {L_new:.4f}"
        )

    logger.info(
        f"[RealtimeGen] BKT計算サマリ: BKT更新回数={bkt_update_count}回, "
        f"既習単元(learned_subunits)数={len(learned_subunits)}件, "
        f"question_bank未一致履歴={unmatched_qb_count}件, ステータス変換不可={unmapped_status_count}件"
    )

    # -----------------------------------------------------------
    # 4. マーカーテキストからの追加キーワード抽出 (既習単元のみ)
    # -----------------------------------------------------------
    if clickhouse_client and db_name and learned_subunits:
        node_cursor = mongo_db["knowledge_nodes"].find({"node_id": {"$gt": 990000000}})
        target_nodes = [str(doc.get("node", "")).strip() for doc in node_cursor if doc.get("node")]

        if target_nodes:
            for k_node in knowledge_map:
                subunit = k_node.get("subunit")
                if subunit not in learned_subunits:
                    continue
                ans_c_id = k_node.get("answer_contents_id")
                p_start = k_node.get("answer_page_start")
                p_end = k_node.get("answer_page_end")
                if not ans_c_id:
                    continue

                sql_marker_text = f"""
                SELECT last_text AS marker_text
                FROM (
                    SELECT
                        marker_position,
                        argMax(CAST(operation_name AS String), timestamp) AS last_op,
                        argMax(CAST(marker_text AS String), timestamp) AS last_text,
                        argMax(CAST(page_no AS Int32), timestamp) AS last_page
                    FROM {db_name}.statements_target
                    WHERE actor_name_id = {{user:String}}
                      AND contents_id = {{answer_contents_id:String}}
                    GROUP BY marker_position
                )
                WHERE last_op = 'ADD_MARKER'
                  AND last_page BETWEEN {{answer_page_start:Integer}} AND {{answer_page_end:Integer}}
                """
                try:
                    marker_result = clickhouse_client.query(sql_marker_text, parameters={
                        "user": user_str,
                        "answer_contents_id": ans_c_id,
                        "answer_page_start": p_start,
                        "answer_page_end": p_end
                    })
                    marker_rows = marker_result.result_rows
                    if marker_rows:
                        combined_text = " ".join([str(r[0]) for r in marker_rows if r[0]])
                        found_nodes = [node for node in target_nodes if node in combined_text]
                        if found_nodes:
                            subunit_keywords.setdefault(subunit, set()).update(found_nodes)
                except Exception as e:
                    logger.error(f"[RealtimeGen] マーカーテキスト取得エラー (ans_c_id='{ans_c_id}'): {e}")

    for sub in list(subunit_keywords.keys()):
        subunit_keywords[sub] = list(subunit_keywords[sub])

    # -----------------------------------------------------------
    # 5. BKT習熟度に基づく出題カテゴリ (LOW / MIDDLE / HIGH / FREE) の選定
    #    LOW: L <= 0.6, MIDDLE: 0.6 < L <= 0.9, HIGH: L > 0.9
    # -----------------------------------------------------------
    middle_list, high_list, low_tuples = [], [], []
    for sub, L in user_bkt_dict.items():
        if L > 0.9:
            high_list.append(sub)
        elif L > 0.6:
            middle_list.append(sub)
        else:
            low_tuples.append((sub, L))

    low_tuples.sort(key=lambda x: x[1])
    low_list = [t[0] for t in low_tuples]

    pool_low = [("LOW", [sub]) for sub in low_list]
    pool_middle = []
    used_high = set()
    for m_sub in middle_list:
        if high_list:
            h_sub = random.choice(high_list)
            pool_middle.append(("MIDDLE", [m_sub, h_sub]))
            used_high.add(h_sub)
        else:
            pool_middle.append(("MIDDLE", [m_sub]))

    pool_high = []
    avail_high = [h for h in high_list if h not in used_high]
    while len(avail_high) >= 3:
        pool_high.append(("HIGH", avail_high[:3]))
        avail_high = avail_high[3:]

    selected_criteria = []
    if pool_low:
        selected_criteria.append(pool_low.pop(0))
    if pool_middle:
        selected_criteria.append(pool_middle.pop(0))
    if pool_high:
        selected_criteria.append(pool_high.pop(0))

    for pool in [pool_low, pool_high, pool_middle]:
        while pool and len(selected_criteria) < 4:
            selected_criteria.append(pool.pop(0))
    while len(selected_criteria) < 4:
        selected_criteria.append(("FREE", []))

    logger.info(f"[RealtimeGen] 最終決定された4問の出題構成 (selected_criteria): {selected_criteria}")

    str_learned = ", ".join(sorted(learned_subunits)) if learned_subunits else "基礎的な計算"

    # -----------------------------------------------------------
    # 6. LLM (gpt-6-sol / temperatureなし) による4問の問題・解答生成とDB保存
    # -----------------------------------------------------------
    prepared_questions_xapi = {}

    for idx, (q_type, sub_list) in enumerate(selected_criteria):
        str_subunit = ", ".join(sub_list) if sub_list else "総合問題"
        derived_unit = unit_lookup.get(sub_list[0], "") if len(sub_list) == 1 else "総合"

        kws = []
        for sub in sub_list:
            if sub in subunit_keywords:
                kws.extend(subunit_keywords[sub])
        kws = list(dict.fromkeys(kws))[:5]
        str_keywords = ", ".join(kws) if kws else "数式、関係式"

        if q_type == "FREE":
            prompt_q = PROMPT_FREE.replace("[GRADE]", grade)
        elif q_type == "LOW":
            prompt_q = (
                PROMPT_LOW
                .replace("[SUBUNIT]", str_subunit)
                .replace("[KEYWORDS]", str_keywords)
                .replace("[LEARNED_SUBUNITS]", str_learned)
            )
        elif q_type == "HIGH":
            prompt_q = (
                PROMPT_MIDDLE_HIGH
                .replace("[SUBUNIT]", str_subunit)
                .replace("[KEYWORDS]", str_keywords)
                .replace("[DIFFICULTY_CONDITION]", CONDITION_REASONING)
                .replace("[LEARNED_SUBUNITS]", str_learned)
            )
        else:  # MIDDLE
            prompt_q = (
                PROMPT_MIDDLE_HIGH
                .replace("[SUBUNIT]", str_subunit)
                .replace("[KEYWORDS]", str_keywords)
                .replace("[DIFFICULTY_CONDITION]", CONDITION_APPLY)
                .replace("[LEARNED_SUBUNITS]", str_learned)
            )

        logger.info(
            f"[RealtimeGen] 第{idx + 1}問 LLM問題生成リクエスト開始: "
            f"model='{model_name}', type='{q_type}', unit='{derived_unit}', subunit='{str_subunit}'"
        )

        q_start_time = time.time()
        try:
            response_q = openai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "あなたは優秀な中学校の数学問題生成システムです。"},
                    {"role": "user", "content": prompt_q}
                ]
            )
            quiz_text = response_q.choices[0].message.content.strip()
            q_end_time = time.time()
            exercise_time = round(q_end_time - q_start_time, 2)
            logger.info(f"[RealtimeGen] 第{idx + 1}問 問題生成成功 ({exercise_time}秒, 文字数={len(quiz_text)})")
        except Exception as e:
            q_end_time = time.time()
            exercise_time = round(q_end_time - q_start_time, 2)
            error_details = str(e)
            logger.error(f"[RealtimeGen] 第{idx + 1}問 問題生成APIエラー ({exercise_time}秒): {error_details}\n{traceback.format_exc()}")
            quiz_text = f"APIエラー詳細 (問題): {error_details}"

        prompt_a = f'''以下はある数学の問題です。 \n {quiz_text} \n
        この問題を解いて、最終的な答えを出しなさい。
        - 日本の{grade}の生徒が理解できる回答を出力すること。
        - $$で囲み mathjax 形式で出力すること。
        - 過程と最終的な答えを分けて出力すること。
        [解答の過程] \n $$ \\begin{{array}}{{l}} XXXX \\end{{array}} $$
        [最終的な答え] \n $$YYYY$$'''

        logger.info(f"[RealtimeGen] 第{idx + 1}問 LLM解答生成リクエスト開始 (model='{model_name}')")
        a_start_time = time.time()
        try:
            response_a = openai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "あなたは優秀な数学教師です。"},
                    {"role": "user", "content": prompt_a}
                ]
            )
            answer_text = response_a.choices[0].message.content.strip()
            a_end_time = time.time()
            answer_time = round(a_end_time - a_start_time, 2)
            logger.info(f"[RealtimeGen] 第{idx + 1}問 解答生成成功 ({answer_time}秒, 文字数={len(answer_text)})")
        except Exception as e:
            a_end_time = time.time()
            answer_time = round(a_end_time - a_start_time, 2)
            error_details = str(e)
            logger.error(f"[RealtimeGen] 第{idx + 1}問 解答生成APIエラー ({answer_time}秒): {error_details}\n{traceback.format_exc()}")
            answer_text = f"APIエラー詳細 (解答): {error_details}"

        primary_sub = sub_list[0] if sub_list else ""
        meta = subunit_bkt_meta.get(primary_sub, {"L0": 0.5, "T": 0.1})
        now_q_jst = datetime.now(JST)

        initial_bkt_entry = {
            "date": now_q_jst.isoformat(),
            "timestamp": now_q_jst,
            "difficulty": 0.5,
            "guess": 0.15,
            "slip": 0.05,
            "L0": round(float(meta["L0"]), 6),
            "T": round(float(meta["T"]), 6)
        }

        try:
            ins_q_res = mongo_db["question_bank"].insert_one({
                "contents_id": "ai_generated",
                "page": user_str,
                "no": str(base_no + idx),
                "quiz_title": f"test_{test_no_str}_{idx + 1}",
                "quiz_text": quiz_text,
                "exercise_creation_time": str(exercise_time),
                "answer_creation_time": str(answer_time),
                "creation_model": model_name,
                "prompt_exercise": prompt_q,
                "prompt_answer": prompt_a,
                "standard_answer": answer_text,
                "figure_explanation": "",
                "unit": derived_unit,
                "subunit": str_subunit,
                "keywords": kws,
                "keywords_marker": kws,
                "difficulty": 0.5,
                "guess": 0.15,
                "slip": 0.05,
                "L0": round(float(meta["L0"]), 6),
                "T": round(float(meta["T"]), 6),
                "bkt_updated_at": now_q_jst,
                "bkt_history": [initial_bkt_entry],
                "rubric": {},
                "previous_quiz": {},
                "school_id": [str(school_id)],
                "course_id": [str(context_id)],
                "user": user_str,
                "show": True,
                "isfigure": False,
                "test_id": test_no_str,
                "test_no": str(idx + 1),
                "cognitive_domain": q_type,
                "timestamp": now_q_jst
            })
            logger.info(
                f"[RealtimeGen] 第{idx + 1}問 question_bank 保存完了: "
                f"_id={ins_q_res.inserted_id}, no={base_no + idx}, test_id={test_no_str}"
            )
        except Exception as e:
            logger.error(f"[RealtimeGen] 第{idx + 1}問 question_bank 保存エラー: {e}\n{traceback.format_exc()}")

        prepared_questions_xapi[str(idx + 1)] = {
            "model": {
                "name": model_name,
                "prompt": prompt_q
            },
            "question_text": quiz_text,
            "question_creation_time": str(exercise_time),
            "answer_text": answer_text,
            "answer_creation_time": str(answer_time)
        }

    try:
        ins_t_res = mongo_db["test"].insert_one({
            "user": user_str,
            "test_id": test_no_str,
            "school_id": school_id,
            "course_id": context_id,
            "session_id": session_id,
            "marker_pages_count": marker_pages_count,
            "timestamp": {"$date": datetime.now(JST).isoformat()}
        })
        logger.info(
            f"[RealtimeGen] テストメタデータ (test) 保存完了: _id={ins_t_res.inserted_id}, "
            f"test_id={test_no_str} (全体所要時間: {time.time() - gen_t0:.2f}秒)"
        )
    except Exception as e:
        logger.error(f"[RealtimeGen] test コレクション保存エラー: {e}\n{traceback.format_exc()}")

    handle_logs(
        user_id=user_str,
        operationname="PreparedTest",
        session_id=session_id,
        lti=lti_state or {"school_id": school_id, "context_id": context_id, "roles": "Learner"},
        value={
            "question_type": "creating",
            "questions": prepared_questions_xapi
        }
    )

    logger.info("------------------------------------------------------------------")
    return True, f"テスト (ID: {test_no_str}) の自動生成が完了しました！"


# ==========================================
# 2. セッション情報読み込みとUI初期化・分岐 (BRiDGE分岐廃止)
# ==========================================
def load_session_info(request: gr.Request):
    user, lti = extract_lti_from_request(request=request)
    roles = lti.get("roles", "")

    if "Learner" in roles:
        user_group = "learner"
    else:
        user_group = "learner"

    logger.info(
        f"[Session] セッション確定: user='{user}', school_id='{lti.get('school_id')}', "
        f"context_id='{lti.get('context_id')}', roles='{roles}' -> user_group='{user_group}'"
    )
    lti, user, dropdown_update, test_map = update_test_dropdown_choices(user, lti)
    return lti, user, dropdown_update, dropdown_update, test_map, user_group


def update_test_dropdown_choices(user, lti):
    try:
        test_docs = list(test_col.find({"user": {"$in": [str(user), "prime"]}}).sort("timestamp", -1))
    except Exception as e:
        logger.error(f"[Dropdown] test コレクションの検索エラー (user='{user}'): {e}\n{traceback.format_exc()}")
        test_docs = []

    test_choices = []
    test_map = {}

    for doc in test_docs:
        t_id = str(doc.get("test_id", "unknown"))
        ts = doc.get("timestamp")

        dt_utc = normalize_tz(ts)
        dt_jst = dt_utc.astimezone(JST)
        yymmdd = dt_jst.strftime("%y%m%d")
        display_name = f"テストサンプル_{t_id}" if doc.get("user") == "prime" else f"{yymmdd}_{t_id}回目"
        test_choices.append(display_name)
        test_map[display_name] = t_id

    logger.info(f"[Dropdown] テスト選択肢更新: user='{user}', 件数={len(test_choices)}件")
    return lti, user, gr.update(choices=test_choices), test_map


def route_ui(user_group):
    logger.debug(f"[UI] コンテナ表示切替: user_group='{user_group}'")
    return (
        gr.update(visible=(user_group == "learner")),
        gr.update(visible=(user_group == "other"))
    )


def log_selected_test(selected_test_name, test_map, user_id, session_id, lti_state):
    if not selected_test_name:
        return
    test_id = test_map.get(selected_test_name, "") if isinstance(test_map, dict) else ""
    handle_logs(
        user_id=user_id,
        operationname="SelectedTest",
        session_id=session_id,
        lti=lti_state,
        value={"test_id": test_id, "test_name": selected_test_name}
    )


# ==========================================
# 状態リセット関数 (final_submit用)
# ==========================================
def reset_test_state():
    logger.info("[State] テスト完了に伴いUI状態をリセットします。")
    return (
        gr.update(visible=False), gr.update(visible=False),
        "お疲れ様でした！結果が送信されました。", "お疲れ様でした！結果が送信されました。",
        gr.update(interactive=True, value=None), gr.update(interactive=True, value=None),
        gr.update(interactive=True), gr.update(interactive=True),
        [], 0, [False]*4, [False]*4, [{} for _ in range(4)],
        gr.update(interactive=False), gr.update(interactive=False),
        gr.update(value=None), gr.update(value=""), gr.update(visible=True), None, None
    )


# ==========================================
# 3. UIコンポーネント用のHTML生成関数 (4問用)
# ==========================================
def build_progress_html(current_idx, q_answered_states):
    color = "#2196f3"
    html = '<div style="background: white; border: 1px solid #777; padding: 15px; border-radius: 4px;">'
    html += '<div style="margin-bottom: 15px; font-weight: normal; color: #333;">テストの進み具合</div>'
    html += '<div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">'

    for i in range(4):
        is_answered = q_answered_states[i]
        bg_color = color if is_answered else "white"
        text_color = "white" if is_answered else color
        scale = "transform: scale(1.15); box-shadow: 0 4px 8px rgba(0,0,0,0.3);" if i == current_idx else ""
        html += f'<div style="width: 50px; height: 35px; border-radius: 25px; border: 2px solid {color}; background-color: {bg_color}; color: {text_color}; display: flex; align-items: center; justify-content: center; font-size: 20px; font-weight: bold; transition: all 0.2s; {scale}">{i+1}</div>'

    html += '</div></div>'
    return html


# ==========================================
# 4. 画面遷移・DBからの問題取得ロジック
# ==========================================
def load_test_data(selected_test_name, test_map, user_id):
    logger.info(f"[LoadTest] テスト読込要求: user='{user_id}', selected_test_name='{selected_test_name}'")
    if not selected_test_name or selected_test_name not in test_map:
        logger.warning(f"[LoadTest] テストが未選択、または test_map に存在しません: selected='{selected_test_name}'")
        return (
            gr.update(visible=False), [], 0, [False]*4, [False]*4, [{} for _ in range(4)],
            "テストを選択してください", gr.update(interactive=True), gr.update(interactive=True)
        )

    test_id = test_map[selected_test_name]
    try:
        questions_cursor = list(exercise_col.find({"user": {"$in": [str(user_id), "prime"]}, "test_id": str(test_id)}))
        logger.info(f"[LoadTest] question_bank 検索結果: test_id='{test_id}', ヒット件数={len(questions_cursor)}件")
    except Exception as e:
        logger.error(f"[LoadTest] question_bank 検索エラー: {e}\n{traceback.format_exc()}")
        questions_cursor = []

    q_dict = {str(doc.get("test_no")): doc for doc in questions_cursor}

    test_data = []
    for i in range(1, 5):
        doc = q_dict.get(str(i), {})
        if not doc:
            logger.warning(f"[LoadTest] test_id='{test_id}' の第{i}問目がDBに見つかりません。")
            test_data.append({
                "q_text": f"第 {i} 問目のデータが見つかりません。",
                "a_text": "データなし",
                "keywords": [],
                "test_id": str(test_id),
                "quiz_title": f"test_{test_id}_{i}"
            })
            continue

        raw_kws = doc.get("keywords_marker") or doc.get("keywords") or doc.get("keyword") or []
        kws = parse_keywords_field(raw_kws)

        test_data.append({
            "q_text": doc.get("quiz_text", ""),
            "a_text": doc.get("standard_answer", ""),
            "keywords": kws,
            "test_id": str(test_id),
            "quiz_title": doc.get("quiz_title", f"test_{test_id}_{i}")
        })

    return (
        gr.update(visible=True), test_data, 0, [False]*4, [False]*4, [{} for _ in range(4)],
        "テストを読み込みました", gr.update(interactive=False), gr.update(interactive=False)
    )


def render_question(idx, test_data, q_revealed, q_answered, q_reflections):
    if not test_data or len(test_data) == 0:
        logger.warning(f"[RenderQuestion] test_data が空のため描画をスキップします (idx={idx})。")
        return [gr.update()] * 13

    q = test_data[idx]
    progress_html = build_progress_html(idx, q_answered)

    q_box = f'<div style="text-align: center;" translate="no"><h2>第{idx+1}問</h2></div><div style="border: 3px solid #2196f3; padding: 24px; border-radius: 8px; text-align: center; background-color: #ffffff; color: #333333; font-size: 18px;" translate="no">\n\n{q["q_text"]}\n\n</div>'
    a_box = f'<div style="text-align: center;" translate="no"><h2>解答</h2></div><div style="padding: 24px; border-radius: 8px; text-align: center; background-color: #ffffff; color: #333333; font-size: 18px;" translate="no">\n\n{q["a_text"]}\n\n</div>'

    raw_kws = q.get("keywords", [])
    has_kws = len(raw_kws) > 0
    kw_choices = raw_kws + ["この中にはない"] if has_kws else []

    is_revealed = q_revealed[idx]
    ref = q_reflections[idx] if isinstance(q_reflections[idx], dict) else {}

    return (
        progress_html, gr.update(value=q_box), gr.update(visible=not is_revealed),
        gr.update(value=a_box, visible=is_revealed), gr.update(visible=(is_revealed and has_kws), choices=kw_choices, value=ref.get("kws", [])),
        gr.update(visible=is_revealed, value=ref.get("und", None)), gr.update(visible=is_revealed, value=ref.get("rat", None)),
        gr.update(visible=is_revealed, value=ref.get("cur", None)), gr.update(visible=is_revealed, value=ref.get("dif", None)),
        gr.update(interactive=(idx > 0)), gr.update(interactive=(idx < 3)), gr.update(interactive=all(q_answered)), gr.update(interactive=(idx == 3))
    )


def reveal_answer(idx, q_revealed, test_data):
    q_revealed[idx] = True
    has_kws = len(test_data[idx].get("keywords", [])) > 0 if test_data and len(test_data) > idx else False
    return (
        q_revealed, gr.update(visible=False), gr.update(visible=True),
        gr.update(visible=has_kws), gr.update(visible=True),
        gr.update(visible=True), gr.update(visible=True), gr.update(visible=True)
    )


def handle_keyword_change(selected_keywords):
    if not selected_keywords:
        return []
    if selected_keywords[-1] == "この中にはない":
        return ["この中にはない"]
    elif "この中にはない" in selected_keywords:
        return [k for k in selected_keywords if k != "この中にはない"]
    return selected_keywords


def record_reflection(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data):
    q_reflections[idx] = {"und": und, "rat": rat, "cur": cur, "dif": dif, "kws": kws}
    has_kws = len(test_data[idx].get("keywords", [])) > 0 if test_data and len(test_data) > idx else False
    is_kws_valid = bool(kws) if has_kws else True

    q_answered[idx] = True if (und and rat and cur and dif and is_kws_valid) else False
    progress_html = build_progress_html(idx, q_answered)
    return q_answered, q_reflections, progress_html, gr.update(interactive=all(q_answered))


def make_reflection_handler(feedback_field):
    def _handler(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data, lti, user, session):
        prev_ref = q_reflections[idx].copy() if (idx < len(q_reflections) and isinstance(q_reflections[idx], dict)) else {}
        ans, ref, prog, btn = record_reflection(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data)

        field_key_map = {
            "Understanding": ("und", und),
            "Rating": ("rat", rat),
            "Difficulty": ("dif", dif),
            "Isincurriculum": ("cur", cur),
            "Relatedtowords": ("kws", kws)
        }
        short_key, current_val = field_key_map[feedback_field]

        if current_val is None or (short_key == "kws" and not current_val and not prev_ref.get("kws")):
            return ans, ref, prog, btn
        if prev_ref.get(short_key) == current_val:
            return ans, ref, prog, btn

        q_no_str = str(idx + 1)
        op_name = f"UpdatedPerQuestionFeedback_{feedback_field}"

        if feedback_field == "Relatedtowords":
            raw_kws = test_data[idx].get("keywords", []) if (test_data and idx < len(test_data)) else []
            kw_choices = raw_kws + ["この中にはない"] if raw_kws else []
            selected_set = set(kws or [])
            words_dict = {}
            for i_kw, kw_desc in enumerate(kw_choices, start=1):
                words_dict[str(i_kw)] = {
                    "description": kw_desc,
                    "value": "True" if kw_desc in selected_set else "False"
                }
            log_payload = {
                "question_no": q_no_str,
                "categories": "Relatedtowords",
                "category_description": "理解できた項目にチェックをつけよう",
                "value": words_dict
            }
        else:
            log_payload = {
                "question_no": q_no_str,
                "type": feedback_field,
                "value": encode_feedback_value(feedback_field, current_val)
            }

        handle_logs(user, op_name, session, lti, log_payload)
        return ans, ref, prog, btn

    return _handler


def nav_prev(idx):
    return max(0, idx - 1), idx + 1


def nav_next(idx):
    return min(3, idx + 1), idx + 1


def finish_test_and_log(test_data, q_reflections, user_id, session_id, lti_state):
    logger.info("[FinishTest] テスト終了画面へ遷移します (FinishedTest)。")
    test_id = test_data[0].get("test_id", "unknown") if (test_data and len(test_data) > 0) else "unknown"
    quiz_fb = {}
    for i in range(4):
        ref = q_reflections[i] if (i < len(q_reflections) and isinstance(q_reflections[i], dict)) else {}
        und_code = encode_feedback_value("Understanding", ref.get("und"))
        rat_code = encode_feedback_value("Rating", ref.get("rat"))
        dif_code = encode_feedback_value("Difficulty", ref.get("dif"))
        cur_code = encode_feedback_value("Isincurriculum", ref.get("cur"))
        kws_val = ref.get("kws", [])
        quiz_fb[str(i + 1)] = {
            "Understanding": und_code,
            "Motivation": rat_code,
            "Difficulty": dif_code,
            "Fluency": cur_code,
            "Relevance": str(len(kws_val)) if isinstance(kws_val, list) else ""
        }

    payload = {
        "test_title": f"test_{test_id}",
        "test_page": str(user_id),
        "test_no": "4",
        "quiz_fb": quiz_fb
    }
    handle_logs(user_id, "FinishedTest", session_id, lti_state, payload)
    return gr.update(visible=False), gr.update(visible=True)


def quit_test_and_log(current_idx, test_data, user_id, session_id, lti_state):
    logger.info("[QuitTest] テスト中断により終了画面へ遷移します (QuittedTest)。")
    handle_logs(user_id, "QuittedTest", session_id, lti_state, {})
    return gr.update(visible=False), gr.update(visible=True)


def validate_and_log_final_rating(rating, user_id, session_id, lti_state):
    is_complete = bool(rating)
    if rating is not None:
        handle_logs(
            user_id,
            "ChangeFinalRating",
            session_id,
            lti_state,
            {"type": "Rating", "value": encode_feedback_value("Rating", rating)}
        )
    return gr.update(interactive=is_complete), gr.update(visible=not is_complete)


def log_overall_comment_change(comment, user_id, session_id, lti_state):
    if comment is not None and str(comment).strip() != "":
        handle_logs(
            user_id,
            "ChangeFeedback",
            session_id,
            lti_state,
            {"type": "Report", "value": str(comment)}
        )


def final_submit_process(comment, rating, lti, user, session):
    log_val = {
        "type": "Rating",
        "value": encode_feedback_value("Rating", rating),
        "rating": rating,
        "comment": comment
    }
    handle_logs(user, "SubmitOverallFeedback", session, lti, log_val)
    return reset_test_state()


# ==========================================
# 5. 学習者自動生成ハンドラ (request.session['user'] 直接参照対応)
# ==========================================
def process_realtime_generation(user_id, lti_state, session_id, request: gr.Request):
    resolved_user, resolved_lti = extract_lti_from_request(request=request, current_lti=lti_state)
    if user_id and str(user_id) not in ("", "3539"):
        resolved_user = str(user_id)
        resolved_lti["user_id"] = resolved_user

    school_id = resolved_lti.get("school_id", "unknown")
    context_id = resolved_lti.get("context_id", "unknown")
    context_title = resolved_lti.get("context_title", "")
    grade = find_grade(context_title)

    # 問題生成開始ログ (StartedCreatingTest)
    handle_logs(
        user_id=resolved_user,
        operationname="StartedCreatingTest",
        session_id=session_id,
        lti=resolved_lti,
        value={"review_type": "creating"}
    )

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("[ProcessGen] 環境変数 OPENAI_API_KEY が設定されていません。")
    client = OpenAI(api_key=api_key)

    logger.info(
        f"[ProcessGen] リアルタイム生成ボタン押下: user='{resolved_user}', school_id='{school_id}', "
        f"context_id='{context_id}', context_title='{context_title}', grade='{grade}', model='{DEFAULT_LLM_MODEL}'"
    )

    success, msg = run_realtime_student_generation(
        user_id=resolved_user,
        school_id=school_id,
        context_id=context_id,
        session_id=session_id,
        mongo_db=quiz_generator_db,
        openai_client=client,
        model_name=DEFAULT_LLM_MODEL,
        grade=grade,
        lti_state=resolved_lti
    )
    lti, user, dropdown_update, test_map = update_test_dropdown_choices(resolved_user, resolved_lti)
    return msg, dropdown_update, dropdown_update, test_map, resolved_lti, resolved_user


# ==========================================
# コンテナ起動時（モジュールimport時）に1回実行 (1週間以内なら自動スキップ)
# ==========================================
initialize_bkt_parameters_on_startup(quiz_generator_db)


# ==========================================
# 6. Gradio Blocks 構築 (BRiDGEコンテナ削除済み)
# ==========================================
with gr.Blocks() as demo:
    # --- States ---
    lti_state = gr.State({})
    user_state = gr.State("")
    session_state = gr.State(str(uuid.uuid4()))
    user_group_state = gr.State("")

    op_state = gr.State("")
    operationname_state = gr.State("")
    log_value_state = gr.State(None)
    idx_log_state = gr.State(None)

    test_map_state = gr.State({})
    test_data_state = gr.State([])
    current_idx_state = gr.State(0)
    q_revealed_state = gr.State([False]*4)
    q_answered_state = gr.State([False]*4)
    q_reflections_state = gr.State([{} for _ in range(4)])

    gr.Markdown(
        """<div style="background-color: #2196f3; padding: 24px; border-radius: 8px; text-align: center; color: black;">
        <h1> $\\Huge \\mathfrak{PRIME} - \\textsf{TESTメーカー}$ </h1>
        </div>"""
    )

    # === 1. Learner (学習者) 向け画面 ===
    with gr.Column(visible=False) as learner_container:
        gr.Markdown("### 🎓 学習者用ダッシュボード")

        with gr.Row():
            test_dropdown_learner = gr.Dropdown(choices=[], label="復習するテストを選択", show_label=True, container=True)

        with gr.Row():
            btn_generate_learner = gr.Button("新しくテストをつくる", variant="primary")
            btn_review_learner = gr.Button("選択したテストを復習する", variant="secondary")

        status_markdown_learner = gr.Markdown("")

    # === 2. その他のユーザー向け画面 ===
    with gr.Column(visible=False) as other_container:
        with gr.Row():
            test_dropdown_other = gr.Dropdown(choices=[], label="", show_label=False, container=False)
        with gr.Row():
            btn_create_other = gr.Button("テストをつくる", interactive=False)
            btn_review_other = gr.Button("つくったテストを復習する", variant="primary")
        status_markdown_other = gr.Markdown("")

    # === 【共通】テスト実行コンテナ ===
    with gr.Column(visible=False) as test_container:
        progress_display = gr.HTML()
        with gr.Row():
            q_display = gr.Markdown()
        btn_show_ans = gr.Button("この問題の解答を見てふりかえる", variant="primary", size="lg")

        with gr.Row():
            a_display = gr.Markdown(visible=False)
            with gr.Column():
                q_keywords = gr.CheckboxGroup(choices=[], label="以下のキーワードのうち、この問題に関連があると思うものを全て選んでください", visible=False)
                q_understanding = gr.Radio(choices=["すべて自力で解けた", "解説を見てわかった", "解説を見てもわからなかった"], label="この問題はどの程度理解できましたか？", visible=False)
                q_difficulty = gr.Radio(choices=["5(難しい)", "4", "3", "2", "1(簡単)"], label="問題はどれくらい難しかったですか？", visible=False)
                q_rating = gr.Radio(choices=["5(役に立った)", "4", "3", "2", "1(役に立たなかった)"], label="この問題はどのぐらい復習の役に立ちましたか？", visible=False)
                q_curriculum = gr.Radio(choices=["含んでいたので、問題が解けなかった", "含んでいたが、問題は解けた", "含んでいなかった"], label="この問題は習っていないことを含んでいましたか？", visible=False)

        with gr.Row():
            btn_prev = gr.Button("前の問題へ", variant="secondary")
            btn_next = gr.Button("次の問題へ", variant="secondary")
            btn_finish = gr.Button("テストを終わる", interactive=False, variant="stop")
        with gr.Row():
            btn_stop_test = gr.Button("テストをやめる", interactive=False)

    # === 【共通】テスト終了・感想コンテナ ===
    with gr.Column(visible=False) as finish_container:
        gr.Markdown("### テスト全体のふりかえり\n今回のテストについての振り返りを行います。")
        diff_warning = gr.HTML("<span style='color:#757575;'>評価を選択すると送信できます（感想は任意）</span>")

        final_rating = gr.Radio(choices=["5(役に立った)", "4", "3", "2", "1(役に立たなかった)"], label="このテストはどのぐらい復習の役に立ちましたか？")
        overall_comment = gr.Textbox(label="感想 (任意)", lines=3, interactive=True)
        btn_final_submit = gr.Button("送信", variant="primary", interactive=False)

    # ==========================================
    # 7. イベントとコールバック定義
    # ==========================================
    demo.load(
        fn=load_session_info, inputs=None,
        outputs=[lti_state, user_state, test_dropdown_learner, test_dropdown_other, test_map_state, user_group_state]
    ).then(
        fn=route_ui, inputs=[user_group_state],
        outputs=[learner_container, other_container]
    ).then(
        fn=lambda: ("StartedSession", str(uuid.uuid4())), inputs=None, outputs=[operationname_state, session_state]
    ).then(
        fn=handle_logs, inputs=[user_state, operationname_state, session_state, lti_state], outputs=None
    )

    # テスト選択ドロップダウン変更時 (SelectedTest)
    test_dropdown_learner.change(
        fn=log_selected_test,
        inputs=[test_dropdown_learner, test_map_state, user_state, session_state, lti_state],
        outputs=None
    )
    test_dropdown_other.change(
        fn=log_selected_test,
        inputs=[test_dropdown_other, test_map_state, user_state, session_state, lti_state],
        outputs=None
    )

    # 問題自動生成ボタン (StartedCreatingTest -> PreparedTest)
    btn_generate_learner.click(
        fn=process_realtime_generation,
        inputs=[user_state, lti_state, session_state],
        outputs=[status_markdown_learner, test_dropdown_learner, test_dropdown_other, test_map_state, lti_state, user_state]
    )

    # 復習開始ボタン (StartedReviewTest)
    btn_review_learner.click(
        fn=load_test_data, inputs=[test_dropdown_learner, test_map_state, user_state],
        outputs=[test_container, test_data_state, current_idx_state, q_revealed_state, q_answered_state, q_reflections_state, status_markdown_learner, test_dropdown_learner, btn_review_learner]
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    ).then(
        fn=lambda: "StartedReviewTest", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda t_name, t_map: {"test_id": t_map.get(t_name, ""), "review_type": "reviewing"} if isinstance(t_map, dict) else {"test_id": "", "review_type": "reviewing"},
        inputs=[test_dropdown_learner, test_map_state], outputs=[log_value_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, log_value_state], outputs=None
    )

    btn_review_other.click(
        fn=load_test_data, inputs=[test_dropdown_other, test_map_state, user_state],
        outputs=[test_container, test_data_state, current_idx_state, q_revealed_state, q_answered_state, q_reflections_state, status_markdown_other, test_dropdown_other, btn_review_other]
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    ).then(
        fn=lambda: "StartedReviewTest", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda t_name, t_map: {"test_id": t_map.get(t_name, ""), "review_type": "reviewing"} if isinstance(t_map, dict) else {"test_id": "", "review_type": "reviewing"},
        inputs=[test_dropdown_other, test_map_state], outputs=[log_value_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, log_value_state], outputs=None
    )

    # 解答表示ボタン (ShowedAnswer)
    btn_show_ans.click(
        fn=reveal_answer, inputs=[current_idx_state, q_revealed_state, test_data_state],
        outputs=[q_revealed_state, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty]
    ).then(
        fn=lambda: "ShowedAnswer", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda idx: {"question_no": str(idx + 1), "student_answer": ""}, inputs=[current_idx_state], outputs=[log_value_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, log_value_state], outputs=None
    )

    # 問題別の各振り返り項目変更時 (UpdatedPerQuestionFeedback_*)
    q_keywords.change(
        fn=handle_keyword_change, inputs=[q_keywords], outputs=[q_keywords]
    ).then(
        fn=make_reflection_handler("Relatedtowords"),
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    q_understanding.change(
        fn=make_reflection_handler("Understanding"),
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    q_rating.change(
        fn=make_reflection_handler("Rating"),
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    q_difficulty.change(
        fn=make_reflection_handler("Difficulty"),
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    q_curriculum.change(
        fn=make_reflection_handler("Isincurriculum"),
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    # 問題移動ボタン (NavigatedPrev / NavigatedNext)
    btn_prev.click(
        fn=nav_prev, inputs=[current_idx_state], outputs=[current_idx_state, idx_log_state]
    ).then(
        fn=lambda: "NavigatedPrev", inputs=None, outputs=[op_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, idx_log_state], outputs=None
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    )

    btn_next.click(
        fn=nav_next, inputs=[current_idx_state], outputs=[current_idx_state, idx_log_state]
    ).then(
        fn=lambda: "NavigatedNext", inputs=None, outputs=[op_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, idx_log_state], outputs=None
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    )

    # テスト終了 (FinishedTest) と テスト中断 (QuittedTest)
    btn_finish.click(
        fn=finish_test_and_log,
        inputs=[test_data_state, q_reflections_state, user_state, session_state, lti_state],
        outputs=[test_container, finish_container]
    )

    btn_stop_test.click(
        fn=quit_test_and_log,
        inputs=[current_idx_state, test_data_state, user_state, session_state, lti_state],
        outputs=[test_container, finish_container]
    )

    # 最終評価・感想の変更イベント (ChangeFinalRating / ChangeFeedback)
    final_rating.change(
        fn=validate_and_log_final_rating,
        inputs=[final_rating, user_state, session_state, lti_state],
        outputs=[btn_final_submit, diff_warning]
    )

    overall_comment.change(
        fn=log_overall_comment_change,
        inputs=[overall_comment, user_state, session_state, lti_state],
        outputs=None
    )

    # 最終送信イベント (SubmitOverallFeedback -> 新セッション StartedSession)
    btn_final_submit.click(
        fn=final_submit_process,
        inputs=[overall_comment, final_rating, lti_state, user_state, session_state],
        outputs=[
            test_container, finish_container, status_markdown_learner, status_markdown_other, test_dropdown_learner, test_dropdown_other, btn_review_learner, btn_review_other,
            test_data_state, current_idx_state, q_revealed_state, q_answered_state, q_reflections_state,
            btn_finish, btn_stop_test, final_rating, overall_comment, diff_warning, log_value_state, idx_log_state
        ]
    ).then(
        fn=lambda: ("StartedSession", str(uuid.uuid4())), inputs=None, outputs=[operationname_state, session_state]
    ).then(
        fn=handle_logs, inputs=[user_state, operationname_state, session_state, lti_state], outputs=None
    )

if __name__ == "__main__":
    logger.info("Gradio サーバーを直接起動します...")
    demo.launch()