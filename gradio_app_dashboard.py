import gradio as gr
from openai import OpenAI
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, UpdateOne
import uuid
import json
import base64
from collections import defaultdict

JST = timezone(timedelta(hours=9))

load_dotenv()

# Mongo 起動を待ってから接続
mongo_client = MongoClient(os.getenv("MONGO_URL"))

# データベースとコレクションを定義
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
history_col = quiz_generator_db["history"]
logs_col = quiz_generator_db["logs"]
session_col = quiz_generator_db["sessions"]

#ltiセッション情報読み込み
def load_session_info(request: gr.Request):
    lti = request.session['user']
    user = lti['user_id']
    return lti, user

#インデックス作成
def setup_indexes():
    ####### summarize_session で使用 #######
    # logs_col (メインのログ)
    # クエリ①, ②, ③の優先順位2に対応
    logs_col.create_index([("session_id", 1), ("operationname", 1), ("timestamp", -1)])
    logs_col.create_index([("session_id", 1), ("timestamp", -1)])
    
    # exercise_col (クイズDB)
    # クエリ③の優先順位1に対応
    exercise_col.create_index([("session_id", 1), ("quiz_text", 1)])
    
    # history_col (レポートDB)
    # クエリ④の「最古のデータ」に対応
    history_col.create_index([("session_id", 1), ("timestamp", 1)])
    
    # session_col (探索用)
    # 最初の session_id 抽出を高速化
    session_col.create_index([("timestamp", 1)])
    session_col.create_index("session_id", unique=True)
    session_col.create_index([("latest_school_id", 1),("prev_contents_id", 1),("prev_page", 1),("prev_no", 1),("latest_context_id", 1),("latest_timestamp", -1)])
    
    return

def handle_logs(user_id, operationname, session, lti, value=None):
    logs_col.insert_one({
        "school_id": lti["school_id"],
        "course_id": lti["context_id"],
        "user": user_id,
        "user_role": lti["roles"],
        "session_id": session,
        "timestamp": datetime.now(JST),
        "operationname": operationname,
        "value": value
    })
    return

# ✅ MongoDBから再読み込みして State と Dropdown を更新する関数
def reload_quiz_map_from_mongo(lti):
    
    documents = list(
        exercise_col.find({
            "school_id": lti["school_id"],
            "$and": [  # <-- 2つの条件を $and で囲む
                {
                    "$or": [
                        {"course_id": lti["context_id"]},
                        {"course_id": "prime"}
                    ]
                },
                {
                    "$or": [
                        {"user": "prime"}
                    ]
                }
            ],
            "show": True
        })
    )

    if not documents:
        return {}, {}, gr.update(choices=[], value=None)
    
    # quiz_text_dict = quiz_title(問題ID:{short_session_id}) → (quiz_textsession, contentid, page, no)
    quiz_text_dict = {}

    for doc in documents:
        text = doc.get("quiz_text")
        contents_id = doc.get("contents_id")
        page = str(doc.get("page"))
        no = str(doc.get("no"))
        title = doc.get("quiz_title")
        school = doc.get("school_id")
        
        if title and text and contents_id and page and no and school:
            # ai_generated 以外の問題群
            if contents_id != "ai_generated":
                quiz_text_dict[title] = (text, contents_id, page, no, school)
    quiz_text_dict["((すべての問題))"] = ("((すべての問題))", "all", "all", "all", school)
    return quiz_text_dict, gr.update(choices=sorted(quiz_text_dict.keys()), value=None)

def summarize_session():
    one_day_ago = datetime.now() - timedelta(days=1)

    ######## 1. 走査すべきsession_idの探索
    # 1日前より古いセッションIDを抽出
    old_session_ids = session_col.distinct("session_id", {"timestamp": {"$lt": one_day_ago}})
    
    # 除外対象以外のsession_idを一括取得
    target_session_ids = logs_col.distinct("session_id", {"session_id": {"$nin": old_session_ids}})
    
    if not target_session_ids:
        print("処理対象のセッションはありません。")
        return

    ######## 2. データの「一括取得」 (Bulk Fetch)
    
    # A) logs_col から操作ログを取得
    logs_cursor = logs_col.find(
        {"session_id": {"$in": target_session_ids}},
        {"session_id": 1, "operationname": 1, "value": 1, "timestamp": 1, "context_id": 1, "school_id": 1},
        sort=[("timestamp", 1)] # 昇順で取得し、後続のループで最新値に更新
    )

    # B) exercise_col から quiz_text をキーに情報を取得 (ai_generatedを除外)
    # 後のマッピングのために一旦全件（または必要範囲）取得
    exercise_cursor = exercise_col.find(
        {"contents_id": {"$ne": "ai_generated"}},
        {"quiz_text": 1, "contents_id": 1, "page": 1, "no": 1}
    )
    # quiz_text をキーにした辞書を作成
    exercise_info_map = {d['quiz_text']: d for d in exercise_cursor if d.get('quiz_text')}

    # C) quiz_mapping (DB3相当) から session_id をキーに情報を取得
    quiz_cursor = exercise_col.find( # ※DB構成に合わせて変数名は調整してください
        {"session_id": {"$in": target_session_ids}, "quiz_text": {"$ne": None, "$ne": ""}}
    )
    quiz_map = {d['session_id']: d for d in quiz_cursor}

    # D) history_col (レポート) を一括取得
    report_cursor = history_col.find(
        {"session_id": {"$in": target_session_ids}, "report.report_text": {"$exists": True, "$ne": ""}},
        sort=[("timestamp", 1)]
    )
    report_map = {}
    for r in report_cursor:
        if r['session_id'] not in report_map:
            report_map[r['session_id']] = r.get("report", {}).get("report_text", "")

    ######## 3. メモリ上でのデータ集約
    session_data_map = defaultdict(lambda: {
        "selected_exercise_value": "",
        "question_text": "",
        "answer_text": "",
        "latest_context_id": None,
        "latest_school_id": None,
        "latest_timestamp": None
    })

    for log in logs_cursor:
        s_id = log['session_id']
        op = log.get('operationname')
        val = log.get('value', "")

        session_data_map[s_id]["latest_context_id"] = log.get("context_id")
        session_data_map[s_id]["latest_school_id"] = log.get("school_id")
        session_data_map[s_id]["latest_timestamp"] = log.get("timestamp")

        if op == "SelectedExercise" and val:
            session_data_map[s_id]["selected_exercise_value"] = val
        elif op == "CreatedQuestion" and val:
            session_data_map[s_id]["question_text"] = val
        elif op == "CreatedAnswer" and val:
            session_data_map[s_id]["answer_text"] = val

    ######## 4. 最終結果の作成と保存
    operations = []
    for s_id in target_session_ids:
        data = session_data_map[s_id]
        
        # クイズマッピングの優先適用
        if s_id in quiz_map:
            data["question_text"] = quiz_map[s_id].get("quiz_text", data["question_text"])
            data["answer_text"] = quiz_map[s_id].get("standard_answer", data["answer_text"])

        # exercise_info_map から prev_ 情報を取得
        ex_val = data["selected_exercise_value"]
        ex_info = exercise_info_map.get(ex_val, {})

        final_doc = {
            "session_id": s_id,
            "prev_contents_id": ex_info.get("contents_id", ""),
            "prev_page": ex_info.get("page", ""),
            "prev_no": ex_info.get("no", ""),
            "question_text": data["question_text"],
            "answer_text": data["answer_text"],
            "student_report": report_map.get(s_id, ""),
            "context_id": data["latest_context_id"],
            "school_id": data["latest_school_id"],
            "latest_timestamp": data["latest_timestamp"]
        }

        # 更新用オペレーションの追加
        operations.append(
            UpdateOne({"session_id": s_id}, {"$set": final_doc}, upsert=True)
        )

    if operations:
        result = session_col.bulk_write(operations)
        print(f"完了: {result.upserted_count}件挿入 / {result.modified_count}件更新")

    return

with gr.Blocks() as demo:
    lti_state = gr.State()  # ここにユーザー情報を保存   
    user_state = gr.State()
    session_state = gr.State()
    operationname_state = gr.State()
    contentsid_state = gr.State()
    page_state = gr.State()
    no_state = gr.State()
    quiz_map_state = gr.State()
    num_rows_state = gr.State()
    description_state = gr.State({})
    dropdown_state = gr.State()

    school_state = gr.State()
    model_state = gr.State("o4-mini")

    gr.Markdown(
        """
        <div style="background-color: #2196f3; padding: 24px; border-radius: 8px; text-align: center; color: black;">
        <h1> $$\\Huge \\mathfrak{PRIME*} - \\textsf{問題閲覧用ダッシュボード}$$ </h1>
        </div>
        """
    )
    
    quiz_dropdown = gr.Dropdown(
        choices=[],
        label="類題が生成された元となる問題を選択してください",
        value=None
    )

    quiz_text_display = gr.Markdown(visible=False)

    with gr.Row():  
        with gr.Column(scale=1):    
            status_msg = gr.Markdown(
                value='検索条件を選んでください',
                visible=True
            )

        with gr.Column(scale=1):
            option_course = gr.Radio(
                choices=["このコースのみ", "すべてのコース"],
                label="検索対象のコース",
                visible=True,
                interactive=True,
                value="このコースのみ"
            )
    with gr.Row():
        with gr.Column(scale=1):
            quiz_browsing_btn = gr.Button("問題を見る/更新する", visible=True, interactive=False)
        with gr.Column(scale=1):
            num_browsing = gr.Markdown(visible=True, value="### ここに検索件数が表示されます")

    result_html = gr.Markdown(visible=False)

    def update_when_dropdown(quiz_title, quiz_text_dict):
        if quiz_title:
            quiz_text, contents_id, page, no, school = quiz_text_dict[quiz_title]
            return (
                gr.update(value=f'<div style="text-align: center;" translate="no"><h1> 選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;" translate="no"> \n{quiz_text} </div>', visible=True),
                quiz_title, contents_id, str(page), str(no)
            )
        # dropboxに何も選択されていない場合（初期状態）
        else:
            return "", "", "", "", "", ""

    quiz_dropdown.change(
        fn=update_when_dropdown,
        inputs=[quiz_dropdown, quiz_map_state],
        outputs=[quiz_text_display, dropdown_state, contentsid_state, page_state, no_state]
    ).then(
        fn=lambda: "DASHBRD_SelectedExercise",
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, dropdown_state],
        outputs=None
    )

    option_course.change(
        fn=lambda: "DASHBOARD_SelectedCourseOption",
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, option_course],
        outputs=None
    )

    def make_html_table(quiz_title, contentsid, page, no, option_course, lti):
        # 検索条件の初期化
        query = {}

        # ① 基本条件：School ID の一致
        query["school_id"] = lti["school_id"]      

        # ② quiz_title による分岐
        if quiz_title != "((すべての問題))":
            query.update({
                "prev_contents_id": contentsid, 
                "prev_page": page, 
                "prev_no": no
            })

        # ③ option_course による分岐
        if option_course == "このコースのみ":
            query["context_id"] = lti["context_id"]
        else:
            query["course_id"] = {"$nin": ["754", "446"]}

        # 検索実行
        target_sessions = list(session_col.find(query).sort("latest_timestamp", -1))

        # 5. ここからHTMLテーブルの作成（後述）
        # --- HTMLテーブルの組み立て ---
        # テーブルのヘッダー部分
        html_rows = []

        for session in target_sessions:
            # question_text から "[問題]" を取り除く (空文字に置換)
            raw_q_text = session.get("question_text", "")
            formatted_q_text = raw_q_text.replace("[問題]", "").strip()

            answer_text = session.get("answer_text", "")
            student_report = session.get("student_report", "")

            # 各行の作成
            row = f"""
                <tr>
                    <td>{formatted_q_text}</td>
                    <td>{answer_text}</td>
                    <td>{student_report}</td>
                </tr>
            """
            html_rows.append(row)

        # 行を結合して、外側の<table>タグで囲む
        table_body = "".join(html_rows)
        full_html = f"""
        <table border="1" cellpadding="10" cellspacing="0" width="100%" translate="no">
            <thead>
                <tr>
                    <th>問題</th>
                    <th>解答</th>
                    <th>生徒のコメント</th>
                </tr>
            </thead>
            <tbody>
                {table_body}
            </tbody>
        </table>
        """

        return gr.update(value=full_html, visible=True), gr.update(value="### " + str(target_sessions) + " 件 見つかりました！"), query

    quiz_browsing_btn.click(
        fn=summarize_session,
        inputs=None,
        outputs=None
    ).then(
        fn=make_html_table,
        inputs=[dropdown_state, contentsid_state, page_state, no_state, option_course, lti_state],
        outputs=[result_html, num_browsing, description_state]
    ).then(
        fn=lambda: "DASHBOARD_BrowsedQuiz",
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, description_state],
        outputs=None
    )

    # ✅ 初回自動読み込み（表示直後に1回実行）
    demo.load(
        fn=setup_indexes,
        inputs=None,
        outputs=None
    ).then(
        fn=load_session_info,
        inputs=None,
        outputs=[lti_state, user_state]
    ).then(
        fn=reload_quiz_map_from_mongo,
        inputs=[lti_state],
        outputs=[quiz_map_state, quiz_dropdown]
    ).then(
        fn=lambda: (
        "StartedSession",
        str(uuid.uuid4())
        ),
        inputs=None,
        outputs=[operationname_state, session_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state],
        outputs=None
    )

demo.queue()