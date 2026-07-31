import gradio as gr
from datetime import datetime, timezone, timedelta
import uuid
import re
import os
import time
import random
import clickhouse_connect
from pymongo import MongoClient
from openai import OpenAI

JST = timezone(timedelta(hours=9))

# ==========================================
# 0. データベース設定
# ==========================================
from dotenv import load_dotenv
load_dotenv()

mongo_client = MongoClient(os.getenv("MONGO_URL"))
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
test_col = quiz_generator_db["test"]
logs_col = quiz_generator_db["logs"]

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

PROMPT_INEPT = """あなたはプロの数学教師です。
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

PROMPT_MASTER_PROFICIENT = """あなたはプロの数学教師です。
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
# 学年抽出ヘルパー関数
# ==========================================
def find_grade(context):
    grades_list = ["中学1年", "中学2年", "中学3年", "高校1年", "高校2年", "高校3年"]
    for grade in grades_list:
        if grade in context:
            return grade
    return "中学3年"

# ==========================================
# 1. リアルタイム問題生成コアロジック
# ==========================================
def run_realtime_student_generation(user_id, school_id, context_id, session_id, mongo_db, openai_client, model_name="gpt-5.5", grade="中学3年"):
    user_str = str(user_id)
    
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
    
    master_questions = mongo_db["question_bank"].find({
        "unit": {"$nin": ["", None]}, 
        "subunit": {"$nin": ["", None]},
        "contents_id": {"$exists": True}
    })
    
    knowledge_map = []
    unit_lookup = {}
    for q in master_questions:
        k_node = {
            "contents_id": q.get("contents_id"),
            "answer_contents_id": q.get("answer_id"),
            "answer_page_start": int(q.get("answer_page_start", 0)) if str(q.get("answer_page_start", "")).isdigit() else 0,
            "answer_page_end": int(q.get("answer_page_end", 0)) if str(q.get("answer_page_end", "")).isdigit() else 0,
            "unit": q.get("unit"),
            "subunit": q.get("subunit")
        }
        knowledge_map.append(k_node)
        unit_lookup[q.get("subunit")] = q.get("unit")
        
    try:
        ch_client = clickhouse_connect.get_client(
            host=os.getenv("BOOKROLL_DATABASE_HOST_1"),
            username=os.getenv("BOOKROLL_DATABASE_USER_1"),
            password=os.getenv("BOOKROLL_DATABASE_PASS_1")
        )
    except Exception as e:
        print(f"[Gen] ClickHouse接続エラー: {e}")
        return False, "データベースに接続できませんでした。"

    user_bkt_dict = {}
    learned_subunits = set()
    L0, T = 0.5, 0.1
    G = 0.25 - (0.2 * 0.5)  # 0.15
    S = 0.01 + (0.08 * 0.5) # 0.05
    
    for k_node in knowledge_map:
        c_id = k_node.get("contents_id")
        subunit = k_node.get("subunit")
        if not c_id or not subunit: continue
        
        learned_subunits.add(subunit)
        
        sql_quiz = """
        SELECT CAST(results_response AS String) AS response
        FROM saikyo_new.statements_target
        WHERE actor_name_id = {user:String}
          AND contents_id = {contents_id:String}
          AND operation_name = 'ANSWER_QUIZ'
        ORDER BY timestamp ASC
        """
        result = ch_client.query(sql_quiz, parameters={"user": user_str, "contents_id": c_id})
        rows = result.result_rows
        
        if not rows: continue
        L = user_bkt_dict.get(subunit, L0)
        
        for row in rows:
            try:
                correct = float(row[0])
            except:
                correct = 0.0

            if correct >= 0.5:
                p_correct = L * (1 - S) + (1 - L) * G
                p_L_obs = (L * (1 - S)) / p_correct if p_correct > 0 else 0
            else:
                p_incorrect = L * S + (1 - L) * (1 - G)
                p_L_obs = (L * S) / p_incorrect if p_incorrect > 0 else 0
            L = p_L_obs + (1 - p_L_obs) * T
            
        user_bkt_dict[subunit] = L

    subunit_keywords = {}
    node_cursor = mongo_db["knowledge_nodes"].find({"node_id": {"$gt": 990000000}})
    target_nodes = [str(doc.get('node', '')).strip() for doc in node_cursor if doc.get('node')]

    for k_node in knowledge_map:
        ans_c_id = k_node.get("answer_contents_id")
        p_start = k_node.get("answer_page_start")
        p_end = k_node.get("answer_page_end")
        subunit = k_node.get("subunit")
        if not ans_c_id or not subunit: continue
        
        sql_marker = """
        SELECT last_text AS marker_text
        FROM (
            SELECT
                marker_position,
                argMax(CAST(operation_name AS String), timestamp) AS last_op,
                argMax(CAST(marker_text AS String), timestamp) AS last_text,
                argMax(CAST(page_no AS Int32), timestamp) AS last_page
            FROM saikyo_new.statements_target
            WHERE actor_name_id = {user:String}
              AND contents_id = {answer_contents_id:String}
            GROUP BY marker_position
        )
        WHERE last_op = 'ADD_MARKER'
          AND last_page BETWEEN {answer_page_start:Integer} AND {answer_page_end:Integer}
        """
        marker_result = ch_client.query(sql_marker, parameters={
            "user": user_str, "answer_contents_id": ans_c_id,
            "answer_page_start": p_start, "answer_page_end": p_end
        })
        
        marker_rows = marker_result.result_rows
        if marker_rows:
            combined_text = " ".join([str(r[0]) for r in marker_rows if r[0]])
            found_nodes = [node for node in target_nodes if node in combined_text]
            if subunit not in subunit_keywords: subunit_keywords[subunit] = set()
            subunit_keywords[subunit].update(found_nodes)

    for sub in subunit_keywords:
        subunit_keywords[sub] = list(subunit_keywords[sub])

    master_list, proficient_list, inept_tuples = [], [], []
    for sub, L in user_bkt_dict.items():
        if L > 0.99: proficient_list.append(sub)
        elif L > 0.9: master_list.append(sub)
        else: inept_tuples.append((sub, L))
        
    inept_tuples.sort(key=lambda x: x[1])
    inept_list = [t[0] for t in inept_tuples]

    pool_inept = [("INEPT", [sub]) for sub in inept_list]
    pool_master = []
    used_prof = set()
    for m_sub in master_list:
        if proficient_list:
            p_sub = random.choice(proficient_list)
            pool_master.append(("MASTER", [m_sub, p_sub]))
            used_prof.add(p_sub)
        else:
            pool_master.append(("MASTER", [m_sub]))
            
    pool_prof = []
    avail_prof = [p for p in proficient_list if p not in used_prof]
    while len(avail_prof) >= 3:
        pool_prof.append(("PROFICIENT", avail_prof[:3]))
        avail_prof = avail_prof[3:]
        
    selected_criteria = []
    if pool_inept: selected_criteria.append(pool_inept.pop(0))
    if pool_master: selected_criteria.append(pool_master.pop(0))
    if pool_prof: selected_criteria.append(pool_prof.pop(0))
        
    for pool in [pool_inept, pool_prof, pool_master]:
        while pool and len(selected_criteria) < 4:
            selected_criteria.append(pool.pop(0))
    while len(selected_criteria) < 4:
        selected_criteria.append(("FREE", []))

    str_learned = ", ".join(list(learned_subunits)) if learned_subunits else "基礎的な計算"

    for idx, (q_type, sub_list) in enumerate(selected_criteria):
        str_subunit = ", ".join(sub_list) if sub_list else "総合問題"
        derived_unit = unit_lookup.get(sub_list[0], "") if len(sub_list) == 1 else "総合"
        
        kws = []
        for sub in sub_list:
            if sub in subunit_keywords: kws.extend(subunit_keywords[sub])
        kws = list(set(kws))[:5]
        str_keywords = ", ".join(kws) if kws else "数式、関係式"
        
        if q_type == "FREE": prompt_q = PROMPT_FREE.replace("[GRADE]", grade)
        elif q_type == "INEPT": prompt_q = PROMPT_INEPT.replace("[SUBUNIT]", str_subunit).replace("[KEYWORDS]", str_keywords).replace("[LEARNED_SUBUNITS]", str_learned)
        elif q_type == "PROFICIENT": prompt_q = PROMPT_MASTER_PROFICIENT.replace("[SUBUNIT]", str_subunit).replace("[KEYWORDS]", str_keywords).replace("[DIFFICULTY_CONDITION]", CONDITION_REASONING).replace("[LEARNED_SUBUNITS]", str_learned)
        else: prompt_q = PROMPT_MASTER_PROFICIENT.replace("[SUBUNIT]", str_subunit).replace("[KEYWORDS]", str_keywords).replace("[DIFFICULTY_CONDITION]", CONDITION_APPLY).replace("[LEARNED_SUBUNITS]", str_learned)
            
        # 問題生成
        q_start_time = time.time()
        try:
            response_q = openai_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "system", "content": "あなたは優秀な中学校の数学問題生成システムです。"}, {"role": "user", "content": prompt_q}],
                temperature=0.7
            )
            quiz_text = response_q.choices[0].message.content.strip()
        except Exception as e:
            # 修正: エラー内容をそのまま問題文のテキストにする & flush=True で強制ログ出力
            error_details = str(e)
            print(f"問題生成エラー: {error_details}", flush=True)
            quiz_text = f"APIエラー詳細 (問題): {error_details}"
        q_end_time = time.time()
        exercise_time = round(q_end_time - q_start_time, 2)

        prompt_a = f'''以下はある数学の問題です。 \n {quiz_text} \n
        この問題を解いて、最終的な答えを出しなさい。
        - 日本の{grade}の生徒が理解できる回答を出力すること。
        - $$で囲み mathjax 形式で出力すること。
        - 過程と最終的な答えを分けて出力すること。
        [解答の過程] \n $$ \\begin{{array}}{{l}} XXXX \\end{{array}} $$
        [最終的な答え] \n $$YYYY$$'''
        
        a_start_time = time.time()
        try:
            response_a = openai_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "system", "content": "あなたは優秀な数学教師です。"}, {"role": "user", "content": prompt_a}],
                temperature=0.3
            )
            answer_text = response_a.choices[0].message.content.strip()
        except Exception as e:
            # 修正: エラー内容をそのまま解答文のテキストにする & flush=True で強制ログ出力
            error_details = str(e)
            print(f"解答生成エラー: {error_details}", flush=True)
            answer_text = f"APIエラー詳細 (解答): {error_details}"
        a_end_time = time.time()
        answer_time = round(a_end_time - a_start_time, 2)

        mongo_db["question_bank"].insert_one({
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
            "keywords": str(kws), 
            "rubric": {}, "previous_quiz": {},
            "school_id": [str(school_id)], "course_id": [str(context_id)],
            "user": user_str, "show": True, "isfigure": False,
            "test_id": test_no_str, "test_no": str(idx + 1),
            "cognitive_domain": q_type, "timestamp": datetime.now(JST)
        })

    mongo_db["test"].insert_one({
        "user": user_str, "test_id": test_no_str,
        "school_id": school_id, "course_id": context_id, "session_id": session_id,
        "timestamp": {"$date": datetime.now(JST).isoformat()}
    })
    return True, f"テスト (ID: {test_no_str}) の自動生成が完了しました！"

# ==========================================
# 2. セッション情報読み込みとUI初期化・分岐
# ==========================================
def load_session_info(request: gr.Request):
    try:
        lti = request.session['user']
        user = lti['user_id']
    except:
        lti = {"user_id": "3539", "school_id": "C126210001533", "context_id": "799", "roles": "Learner", "context_title": "中学1年数学"}
        user = lti['user_id']

    school_id = lti.get("school_id", "")
    context_id = str(lti.get("context_id", ""))
    roles = lti.get("roles", "")

    if school_id == "BRiDGE" and context_id == "867":
        user_group = "bridge_867"
    elif "Learner" in roles:
        user_group = "learner"
    else:
        user_group = "learner"
        
    lti, user, dropdown_update, test_map = update_test_dropdown_choices(user, lti)
    return lti, user, dropdown_update, dropdown_update, test_map, user_group

def update_test_dropdown_choices(user, lti):
    test_docs = list(test_col.find({"user": {"$in": [str(user), "prime"]}}).sort("timestamp", -1))
    test_choices = []
    test_map = {}
    
    for doc in test_docs:
        t_id = str(doc.get("test_id", "unknown"))
        ts = doc.get("timestamp")
        
        if isinstance(ts, dict) and "$date" in ts:
            dt_utc = datetime.fromisoformat(ts["$date"].replace("Z", "+00:00"))
        elif isinstance(ts, datetime):
            dt_utc = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        elif isinstance(ts, str):
            dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt_utc = datetime.now(timezone.utc)
        
        dt_jst = dt_utc.astimezone(JST)
        yymmdd = dt_jst.strftime("%y%m%d")
        display_name = f"テストサンプル_{t_id}" if doc.get("user") == "prime" else f"{yymmdd}_{t_id}回目"
        test_choices.append(display_name)
        test_map[display_name] = t_id
        
    return lti, user, gr.update(choices=test_choices), test_map

def route_ui(user_group):
    return (
        gr.update(visible=(user_group == "bridge_867")),
        gr.update(visible=(user_group == "learner")),
        gr.update(visible=(user_group == "other"))
    )

def handle_logs(user_id, operationname, session_id, lti, value=None):
    if isinstance(lti, str):
        school_id, course_id, roles = "unknown", "unknown", ""
    else:
        school_id = lti.get("school_id", "unknown")
        course_id = lti.get("context_id", "unknown")
        roles = lti.get("roles", "")

    logs_col.insert_one({
        "school_id": school_id, "course_id": course_id, "user": str(user_id),
        "user_role": roles, "session_id": session_id, "timestamp": datetime.now(JST),
        "operationname": operationname, "value": value
    })
    print(f"[LOG] {operationname}: {value} (User: {user_id})")

# ==========================================
# 状態リセット関数 (final_submit用)
# ==========================================
def reset_test_state():
    return (
        gr.update(visible=False), gr.update(visible=False), 
        "お疲れ様でした！結果が送信されました。", "お疲れ様でした！結果が送信されました。", # Learner用、Other用のメッセージ
        gr.update(interactive=True, value=None), gr.update(interactive=True, value=None), 
        gr.update(interactive=True), gr.update(interactive=True), # Learner用、Other用の復習ボタン有効化
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
    if not selected_test_name or selected_test_name not in test_map:
        return (
            gr.update(visible=False), [], 0, [False]*4, [False]*4, [{} for _ in range(4)], 
            "テストを選択してください", gr.update(interactive=True), gr.update(interactive=True)
        )
        
    test_id = test_map[selected_test_name]
    questions_cursor = exercise_col.find({"user": {"$in": [str(user_id), "prime"]}, "test_id": str(test_id)})
    q_dict = {str(doc.get("test_no")): doc for doc in questions_cursor}
    
    test_data = []
    for i in range(1, 5):
        doc = q_dict.get(str(i), {})
        if not doc:
            test_data.append({"q_text": f"第 {i} 問目のデータが見つかりません。", "a_text": "データなし", "keywords": []})
            continue
        raw_kws = doc.get("keywords_marker", [])
        if isinstance(raw_kws, str):
            try: raw_kws = eval(raw_kws)
            except: raw_kws = []
        kws = [str(k) for k in raw_kws if k]
            
        test_data.append({
            "q_text": doc.get("quiz_text", ""),
            "a_text": doc.get("standard_answer", ""),
            "keywords": kws
        })
        
    return (
        gr.update(visible=True), test_data, 0, [False]*4, [False]*4, [{} for _ in range(4)], 
        "テストを読み込みました", gr.update(interactive=False), gr.update(interactive=False)
    )

def render_question(idx, test_data, q_revealed, q_answered, q_reflections):
    if not test_data or len(test_data) == 0:
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
    return (q_revealed, gr.update(visible=False), gr.update(visible=True), gr.update(visible=has_kws), gr.update(visible=True), gr.update(visible=True), gr.update(visible=True), gr.update(visible=True))

def handle_keyword_change(selected_keywords):
    if not selected_keywords: return []
    if selected_keywords[-1] == "この中にはない": return ["この中にはない"]
    elif "この中にはない" in selected_keywords: return [k for k in selected_keywords if k != "この中にはない"]
    return selected_keywords

def record_reflection(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data):
    q_reflections[idx] = {"und": und, "rat": rat, "cur": cur, "dif": dif, "kws": kws}
    has_kws = len(test_data[idx].get("keywords", [])) > 0 if test_data and len(test_data) > idx else False
    is_kws_valid = bool(kws) if has_kws else True
    
    q_answered[idx] = True if (und and rat and cur and dif and is_kws_valid) else False
    progress_html = build_progress_html(idx, q_answered)
    return q_answered, q_reflections, progress_html, gr.update(interactive=all(q_answered))

def record_and_log_reflection(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data, lti, user, session):
    ans, ref, prog, btn = record_reflection(und, rat, cur, dif, kws, idx, q_answered, q_reflections, test_data)
    handle_logs(user, "SubmitPerQuestionFeedback", session, lti, {"question_no": idx + 1, "selections": [und, rat, cur, dif, kws]})
    return ans, ref, prog, btn

def nav_prev(idx): return max(0, idx - 1), idx + 1
def nav_next(idx): return min(3, idx + 1), idx + 1

def finish_test(test_data):
    return (gr.update(visible=False), gr.update(visible=True))

def validate_final_reflection(rating):
    is_complete = bool(rating)
    return (gr.update(interactive=is_complete), gr.update(visible=not is_complete))

def final_submit_process(comment, rating, lti, user, session):
    log_val = {"rating": rating, "comment": comment}
    handle_logs(user, "SubmitOverallFeedback", session, lti, log_val)
    return reset_test_state()

# ==========================================
# 5. 学習者自動生成ハンドラ
# ==========================================
def process_realtime_generation(user_id, lti_state, session_id):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    context_title = lti_state.get("context_title", "")
    grade = find_grade(context_title)
    
    success, msg = run_realtime_student_generation(
        user_id=user_id, school_id=lti_state.get("school_id", "unknown"),
        context_id=lti_state.get("context_id", "unknown"), session_id=session_id,
        mongo_db=quiz_generator_db, openai_client=client, model_name="gpt-5.5",
        grade=grade
    )
    lti, user, dropdown_update, test_map = update_test_dropdown_choices(user_id, lti_state)
    return msg, dropdown_update, dropdown_update, test_map

# ==========================================
# 6. Gradio Blocks 構築
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
    
    # === 1. BRiDGE 向け画面 ===
    with gr.Column(visible=False) as bridge_container:
        gr.Markdown("### BRiDGE画面（構築中）")

    # === 2. Learner (学習者) 向け画面 ===
    with gr.Column(visible=False) as learner_container:
        gr.Markdown("### 🎓 学習者用ダッシュボード")
        
        with gr.Row():
            test_dropdown_learner = gr.Dropdown(choices=[], label="復習するテストを選択", show_label=True, container=True)
            
        with gr.Row():
            btn_generate_learner = gr.Button("新しくテストをつくる", variant="primary")
            btn_review_learner = gr.Button("選択したテストを復習する", variant="secondary")
                
        status_markdown_learner = gr.Markdown("")

    # === 3. その他のユーザー向け画面 ===
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
        outputs=[bridge_container, learner_container, other_container]
    ).then(
        fn=lambda: ("StartedSession", str(uuid.uuid4())), inputs=None, outputs=[operationname_state, session_state]
    ).then(
        fn=handle_logs, inputs=[user_state, operationname_state, session_state, lti_state], outputs=None
    )

    btn_generate_learner.click(
        fn=process_realtime_generation,
        inputs=[user_state, lti_state, session_state],
        outputs=[status_markdown_learner, test_dropdown_learner, test_dropdown_other, test_map_state]
    )

    # --- 修正: 復習開始処理の出し分け対応 ---
    btn_review_learner.click(
        fn=load_test_data, inputs=[test_dropdown_learner, test_map_state, user_state],
        outputs=[test_container, test_data_state, current_idx_state, q_revealed_state, q_answered_state, q_reflections_state, status_markdown_learner, test_dropdown_learner, btn_review_learner]
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    ).then(
        fn=lambda: "StartReviewTest", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda t_name, t_map: t_map.get(t_name, None) if isinstance(t_map, dict) else None, inputs=[test_dropdown_learner, test_map_state], outputs=[log_value_state]
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
        fn=lambda: "StartReviewTest", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda t_name, t_map: t_map.get(t_name, None) if isinstance(t_map, dict) else None, inputs=[test_dropdown_other, test_map_state], outputs=[log_value_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, log_value_state], outputs=None
    )

    # ----------------------------------------

    btn_show_ans.click(
        fn=reveal_answer, inputs=[current_idx_state, q_revealed_state, test_data_state],
        outputs=[q_revealed_state, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty]
    ).then(
        fn=lambda: "ShowAnswer", inputs=None, outputs=[op_state]
    ).then(
        fn=lambda idx: idx + 1, inputs=[current_idx_state], outputs=[log_value_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, log_value_state], outputs=None
    )

    q_keywords.change(
        fn=handle_keyword_change, inputs=[q_keywords], outputs=[q_keywords]
    ).then(
        fn=record_and_log_reflection,
        inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
        outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
    )

    for comp in [q_understanding, q_rating, q_curriculum, q_difficulty]:
        comp.change(
            fn=record_and_log_reflection,
            inputs=[q_understanding, q_rating, q_curriculum, q_difficulty, q_keywords, current_idx_state, q_answered_state, q_reflections_state, test_data_state, lti_state, user_state, session_state],
            outputs=[q_answered_state, q_reflections_state, progress_display, btn_finish]
        )

    btn_prev.click(
        fn=nav_prev, inputs=[current_idx_state], outputs=[current_idx_state, idx_log_state]
    ).then(
        fn=lambda: "NavigatePrev", inputs=None, outputs=[op_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, idx_log_state], outputs=None
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    )

    btn_next.click(
        fn=nav_next, inputs=[current_idx_state], outputs=[current_idx_state, idx_log_state]
    ).then(
        fn=lambda: "NavigateNext", inputs=None, outputs=[op_state]
    ).then(
        fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state, idx_log_state], outputs=None
    ).then(
        fn=render_question, inputs=[current_idx_state, test_data_state, q_revealed_state, q_answered_state, q_reflections_state],
        outputs=[progress_display, q_display, btn_show_ans, a_display, q_keywords, q_understanding, q_rating, q_curriculum, q_difficulty, btn_prev, btn_next, btn_finish, btn_stop_test]
    )

    for btn_trigger in [btn_finish, btn_stop_test]:
        btn_trigger.click(
            fn=finish_test, inputs=[test_data_state], outputs=[test_container, finish_container]
        ).then(
            fn=lambda: "FinishTest", inputs=None, outputs=[op_state]
        ).then(
            fn=handle_logs, inputs=[user_state, op_state, session_state, lti_state], outputs=None
        )
    
    all_final_inputs = [final_rating]
    for comp in all_final_inputs:
        comp.change(fn=validate_final_reflection, inputs=all_final_inputs, outputs=[btn_final_submit, diff_warning])
    
    btn_final_submit.click(
        fn=final_submit_process,
        inputs=[overall_comment, final_rating, lti_state, user_state, session_state],
        # 修正: status_markdown_other と btn_review_other もoutputsに追加してリセット
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
    demo.launch()