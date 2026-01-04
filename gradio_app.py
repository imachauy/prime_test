import gradio as gr
from openai import OpenAI
import time
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import uuid
import json
import base64
import clickhouse_connect

JST = timezone(timedelta(hours=9))

load_dotenv()

# Mongo 起動を待ってから接続
mongo_client = MongoClient(os.getenv("MONGO_URL"))

# データベースとコレクションを定義
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
history_col = quiz_generator_db["history"]
logs_col = quiz_generator_db["logs"]

#ltiセッション情報読み込み
def load_session_info(request: gr.Request):
    lti = request.session['user']
    user = lti['user_id']
    return lti, user

# openaiのapi情報
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
models = ["gpt-3.5-turbo", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "o1-preview", "o1-mini", "o3-mini"]

# genaiのapiを走らせる
def gpt_exection(model, query):
    '''
    str: model, str: query
    '''
    completion = openai_client.chat.completions.create(
        model=model,
        messages=[
            {
            "role": "user", "content": query
            }
        ]
    ) 
    return completion.choices[0].message.content

def handle_answer(save, user_id, session, contents_id, page, no, tags, school, new_contents_id, new_page, new_no, user_answer, understanding, rating, difficulty, fluency, relevance, new_checkbox, lti, report_type, report_text):
    report = {
        "report_type": report_type,
        "report_text": report_text
    }
    if save:
        previous_quiz = {
            "school_id": school,
            "contents_id": contents_id,
            "page": page,
            "no": no,
            "tags": tags
        }
    else:
        previous_quiz = {}
    
    history_doc = {
        "user": user_id,
        "user_role": lti["roles"],
        "contents_id": new_contents_id,
        "page": new_page,
        "no": new_no,
        "user_answer": user_answer,
        "understanding": understanding,
        "understanding_details": new_checkbox,
        "rating": rating,
        "difficulty": difficulty,
        "fluency": fluency,
        "relevance": relevance,
        "report": report,
        "previous_quiz": previous_quiz,
        "timestamp": datetime.now(JST),
        "school_id": lti["school_id"],
        "course_id": lti["context_id"],
        "session_id": session
    }
    history_col.insert_one(history_doc)
    return

def handle_exercise(save, no, quiz_text, standard_answer, user, exercise_creation_time, answer_creation_time, model, session, lti, prompt_exercise, prompt_answer, rubric=None, figure_explanation=""):
    if save:
        title = gpt_exection("gpt-4.1-nano", "以下の問題に短いタイトルをつけてください。タイトルのみを出力してください。\n{}".format(quiz_text))
        # rubricがNoneなら空にする
        rubric = rubric or {}

        # 保存対象の小問構造
        new_entry = {
            "contents_id": "ai_generated",
            "page": user,
            "no": no,
            "quiz_title": title,
            "quiz_text": quiz_text,
            "standard_answer": standard_answer,
            "figure_explanation": figure_explanation,
            "rubric": rubric,
            "exercise_creation_time": exercise_creation_time,
            "answer_creation_time": answer_creation_time,
            "creation_model": model,
            "prompt_exercise": prompt_exercise,
            "prompt_answer": prompt_answer,
            "school_id": lti["school_id"],
            "course_id": lti["context_id"],
            "user": user,
            "session_id": session,
            "timestamp": datetime.now(JST),
            "show": True
        }
        exercise_col.insert_one(new_entry)
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

#学年を特定する
def find_grade(context):
    grades_list = ["中学1年", "中学2年", "中学3年", "高校1年", "高校2年", "高校3年"]
    for grade in grades_list:
        if grade in context:
            return grade
    return "中学3年"

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
                        {"user": lti["user_id"]},
                        {"user": "prime"}
                    ]
                }
            ],
            "show": True
        })
    )

    if not documents:
        return {}, {}, gr.update(choices=[], value=None)

    def shorten_sessionid(sessionid, n=5):
        sessionid = uuid.UUID(sessionid)
        s = base64.urlsafe_b64encode(sessionid.bytes)
        return s.decode('ascii').rstrip('=')[:n]
    
    # quiz_text_dict = quiz_title(問題ID:{short_session_id}) → (quiz_textsession, contentid, page, no)
    quiz_text_dict = {}

    for doc in documents:
        text = doc.get("quiz_text")
        contents_id = doc.get("contents_id")
        page = str(doc.get("page"))
        no = str(doc.get("no"))
        title = doc.get("quiz_title")
        school = doc.get("school_id")
        if contents_id == "ai_generated":
            sessionid = shorten_sessionid(doc.get("session_id"))
            title = "類題" + f"{int(no):04d}: " + title + " (問題ID:{})".format(sessionid)
        
        if title and text and contents_id and page and no and school:
            quiz_text_dict[title] = (text, contents_id, page, no, school)
    return quiz_text_dict, gr.update(choices=sorted(quiz_text_dict.keys()), value=None)

phrases = [
    #応援
    "がんばるでありMath！",
    "自信を持って解きMath！",
    "あきらめないで、まだ解けMath！",
    "難問？ いや、やればできMath！",
    "ステップアップの数式は、君の中にありMath！",
    "失敗しても大丈夫、それも経験値に変えMath！",
    "つまずいた数だけ、君は強くなりMath！",
    "苦手だって、向き合えば得意に変わりMath！",
    "一歩ずつ前に進めば、ゴールに近づきMath！",
    "何度でも挑戦できる、それが君の強さでありMath！",
    "考え抜いた先に、答えが待っていMath！",
    "その粘り強さ、必ず実りMath！",
    "数学と友達になれMath！",
    "君なら絶対、乗り越えられMath！",
    #注意
    "答えだけじゃない、プロセスも大事にしMath！",
    "検算こそ、正解率を高めMath！",
    "凡ミスこそ、一番の強敵でありMath！",
    "何を問われているか、しっかり読みMath！",
    "使ってない条件はないか、見直しMath！",
    "符号に気をつけMath！",
    "単位の書き忘れ、命取りになりMath！"
]

# userのこれまでのresultを入手する
def get_result_from_db(school, contents_id, page, no, user, lti):
    
    # 該当の問題に取り組んだ数
    num_workingquiz = 0

    # BookRollのデータを取得
    if lti["school_id"] == os.getenv("LTI_CONSUMER_KEY_1"):
        clickhouse_client = clickhouse_connect.get_client(
            host=os.getenv("BOOKROLL_DATABASE_HOST_1"), 
            username=os.getenv("BOOKROLL_DATABASE_USER_1"), 
            password=os.getenv("BOOKROLL_DATABASE_PASS_1")
        )

        sql = """
        SELECT actor_name_id, contents_id, page_no, description, timestamp
        FROM saikyo_new.statements_target
        WHERE operation_name='ANSWER_QUIZ'
        AND actor_name_id={user:String}
        AND contents_id={contents_id:String}
        AND page_no={page:String}
        """
        params = {
        "user": str(user),  # userの値をセット
        "contents_id": str(contents_id),  # contents_idも同様に
        "page": str(page)
        }
        brquizdata = clickhouse_client.query(sql, params)
        result = brquizdata.result_rows
        print(result)
        num_workingquiz += len(brquizdata.result_rows)
    
    # MongoDBクエリ
    query = {
        "school_id": lti["school_id"],
        "user": user,
        "contents_id": contents_id,
        "page": page,
        "no": no
    }
    matching_docs = list(history_col.find(query))
    num_workingquiz += len(matching_docs)

    # 該当の問題を復習した数
    num_reviewquiz = 0
    query = {
        "user": user,
        "previous_quiz.school_id": lti["school_id"],
        "previous_quiz.contents_id": contents_id,
        "previous_quiz.page": page,
        "previous_quiz.no": no
    }
    matching_docs = list(history_col.find(query))
    num_reviewquiz += len(matching_docs)

    return num_workingquiz, num_reviewquiz

reason = ["この問題についてはよくできています。さらに知識を応用した問題で復習しましょう！\n",
          "最後の最後でミスをしています。最後まで気を抜かずに、しっかり解き切りましょう！\n",
          "途中までよくできています。元の問題を解くために必要なステップを確認するために、以下の問題に取り組みましょう！\n",
          "ところどころ間違っているようです。怪しいポイントを確認して、カンペキに解けるようになりましょう！\n",
          "ちょっと難しすぎましたね。でも大丈夫。ひとつずつ確認しましょう。\n"]

def classify_binary(binary_string, knowledge):
    if all(c == '1' for c in binary_string):  # すべて1の場合
        text = """生徒がこの問題で分からなかった部分はないので、この問題に使われている知識に別の知識を組み合わせた新しい問題を作ってください。"""
        return text, 0
    elif all(c == '0' for c in binary_string):  # すべて0の場合
        text = """生徒はこの問題が全くわかっていないので、この問題に使われている最初の知識を簡単に復習する問題を作ってください。"""
        return text, 4
    elif binary_string.endswith('0') and all(c == '1' for c in binary_string[:-1]):  # 最後だけが0の場合
        text = "生徒は最後の最後でミスをしているので、最後まで解き切らせるような問題を作ってください。"
        return text, 1
    elif '01' not in binary_string:  # 1が左、0が右（交互にならない）
        text = "生徒は問題の「{}」から「{}」までの知識はわかっているので、「{}」から「{}」までの知識を扱う問題を作成してください。".format(knowledge[0], knowledge[binary_string.index('0')], knowledge[0], knowledge[binary_string.index('0') + 1])
        return text, 2
    else:  # 1と0が交互の場合、最初の0の位置を返す
        text = "生徒はところどころ理解していない部分があるので、「{}」を確認する問題を作成してください。".format(knowledge[binary_string.index('0') + 1])
        return text, 3

def check_if_solvable(question, knowledge, num, model, grade):
    if num == "type1":
        prompt = '''
        以下はある数学の問題です。 \n {} \n
        この問題を解いて、最終的な答えを出しなさい。
        - 以下の知識を用いても良い。 \n {} \n
        - 日本の{}の生徒が理解できる回答を出力すること。
        - 以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
        例：$$\\int_{}^{} f(x)\\\\,dx = F(b) - F(a)$$
        例：$$\\beta + \\gamma \\{} \\\\ \\alpha \\{}$$
        - ただし、以下の点に注意すること。
            - mathjaxフォーマットの&は使わないこと。
            - \\text で括らないこと。
            - <, >の２つの記号は、必ず"<\\," ">\\," という形で出力すること。
        - 解答の過程を出力すること。
        - 以下のフォーマットで、XXXXに解答の過程、YYYYに最終的な答えを挿入して答えること。
        [解答の過程] \n
        $$ \\begin{}{}{} XXXX \\end{}{} $$
        [最終的な答え] \n
        $$YYYY$$
        '''.format(question, knowledge, grade, r"{a}", r"{b}", r"text{の値から、}", r"text{を求める}", "{array", "}{l", "}", "{array", "}")
    start_time = time.time()
    ans = gpt_exection(model, prompt)
    end_time = time.time()
    elapsed_time_solve = end_time - start_time
    return ans, prompt, elapsed_time_solve

def execute0006_ks(question, answer, knowledge, tags, model, quiz_base, grade):
    stats_bit = ''.join(str(1 if tag == '_o_' else 0) for tag in tags)
    stats, bittype = classify_binary(stats_bit, knowledge)
    base1 = '''
    生徒がある問題を解きました。
    '''

    base2 = '''
    この問題の設定は次のとおりです。 \n {} \n
    '''.format(quiz_base)
    
    prompt_type = 1

    if stats == "生徒がこの問題で分からなかった部分はないので、この問題に使われている知識に別の知識を組み合わせた新しい問題を作ってください。":
        prompt_type = random.choice([1, 2])
    
    if prompt_type == 1:
        prompt_main = '''
        問題に必要な数学的思考・計算技術・注意すべき点は次の通りです。 \n {} \n
        {} \n
        '''.format(knowledge, stats)
    else:
        prompt_main = '''
        もとの問題の問題文は次の通りです。 \n {} \n
        この問題の数値や条件を変えて、新たな問題を作成してください。 \n
        '''.format(question)

    condition = '''
    - 新しい問題のみを結果として出力すること。
    - 日本の{}の生徒が理解できる問題を出題すること。
    - 問題は以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
    例：$$\\int_{}^{} f(x)\\\\,dx = F(b) - F(a)$$
    例：$$\\beta + \\gamma \\{} \\\\ \\alpha \\{}$$
    ただし、以下の点に注意すること。
    - mathjaxフォーマットの&は使わないこと。
    - \\text で括らないこと。
    - <, >の２つの記号は、必ず"<\\," ">\\," という形で出力すること。
    以下のフォーマットで、XXXXに問題を挿入して、左揃えで答えること。
    [問題] \n
    $$ \\begin{}{}{} XXXX \\end{}{} $$
    '''.format(grade, r"{a}", r"{b}", r"text{の値から、}", r"text{を求める}", "{array", "}{l", "}", "{array", "}")

    if quiz_base == "":
        prompt = base1 + prompt_main + condition
    else:
        prompt = base1 + base2 + prompt_main + condition

    start_time = time.time()
    ans = gpt_exection(model, prompt)
    end_time = time.time()
    elapsed_time_creation = end_time - start_time
    return reason[bittype], ans, f"{elapsed_time_creation:.2f}", prompt

# rubricの説明を抽出する関数
def get_main_explanations(quiz_title, quiz_text_dict):
    quiz_text, contents_id, page, no, school = quiz_text_dict[quiz_title]
    exercise_info = exercise_col.find_one({"school_id": school, "contents_id": contents_id, "page": page, "no": no})
    rubrics = exercise_info.get("rubric", {})
    main_list = [item["main"] for item in rubrics.values() if "main" in item]
    if (len(main_list) > 0) and ("上記の項目について、ひとつも理解できなかった" not in main_list):
        main_list.append("上記の項目について、ひとつも理解できなかった")
    return main_list

def initial_register():
    #quiz.json読み込み
    quiz_path = os.path.join(os.path.dirname(__file__), "static", "quiz.json")

    with open(quiz_path, encoding="utf-8") as f:
        quiz_list = json.load(f)

    for quiz in quiz_list:
        contents_id = quiz["contents_id"]
        page = quiz["page"]
        no = quiz["no"]
        school = quiz["school_id"]
        user = quiz["user"]
        # 追加または置き換え
        exercise_col.replace_one({"contents_id": contents_id, "page": page, "no": no, "school_id": school, "user": user}, quiz, upsert=True)

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
    exercise_saving_state = gr.State(True)

    exercise_state = gr.State()
    exercise_creation_time_state = gr.State()
    answer_creation_time_state = gr.State()
    overall_creation_time_state = gr.State()
    prompt_exercise_state = gr.State("")
    prompt_answer_state = gr.State("")
    new_contentsid_state = gr.State()
    new_page_state = gr.State()
    new_no_state = gr.State()
    tags_state = gr.State()
    grade_state = gr.State()
    school_state = gr.State()
    model_state = gr.State("o4-mini")
    gen_state = gr.State()

    gr.Markdown(
        """
        <div style="background-color: #2196f3; padding: 24px; border-radius: 8px; text-align: center; color: black;">
        <h1> $$\\Huge \\mathfrak{PRIME} - \\textsf{AI数学復習エンジン}$$ </h1>
        </div>
        """
    )
    
    report_result = gr.Markdown(
        "### 正常に送信されました！",
        visible=False
    )
    
    with gr.Row():
        with gr.Column(scale=3): 
            title = gr.Markdown(
                "## " + random.choice(phrases),
                visible=True
            )

        with gr.Column(scale=2): 
            vanish_btn = gr.Button(
                value="応援メッセージを消す",
                visible=True,
                interactive=True,
                variant="secondary"
            )
    title_state = gr.State()

    quiz_dropdown = gr.Dropdown(
        choices=[],
        label="まずは、復習する問題を選んでください",
        value=None
    )
    dropdown_state = gr.State()

    quiz_text_display = gr.Markdown(visible=False)

    with gr.Row():  
        with gr.Column(scale=1):    
            status_msg = gr.Markdown(
                value='',
                visible=False
            )

        with gr.Column(scale=1):
            checkboxes = gr.CheckboxGroup(
                choices=[], 
                label="できたポイントをチェックしよう", 
                visible=False, 
                show_label = False
            )
    status_msg_state = gr.State()
    checkbox_state = gr.State()
    checkbox_all_items_state = gr.State()
    current_checkbox_state = gr.State([])
    check_flaw_state = gr.State()
    new_checkbox_all_items_state = gr.State()
    new_checkbox_state = gr.State()
    new_current_checkbox_state = gr.State([])
    cnt_work_state = gr.State()
    cnt_review_state = gr.State()
    
    with gr.Row():  
        with gr.Column(scale=1):    
            gen_quiz_btn = gr.Button("類題をつくる（まだ押せません）", visible=True, interactive=False, variant="stop")

        with gr.Column(scale=1):
            rev_quiz_btn = gr.Button("そのまま解く（まだ押せません）", visible=True, interactive=False, variant="stop")

    with gr.Row():
        with gr.Column(scale=1):
            exercise_output = gr.Markdown(
                value="復習問題はここに出てきます",
                visible=True
            )

        with gr.Column(scale=1):
            student_answer = gr.Textbox(
                label="左側の問題の解答を書いてみよう",
                lines=1,
                placeholder="(まだ入力できません)",
                visible=True,
                interactive=False
            )

    answer_btn = gr.Button("模範解答を表示", visible=False)

    with gr.Row():
        with gr.Column(scale=3):
            answer_output = gr.Markdown(
                "",
                visible=False
            )
        answer_state = gr.State()

        with gr.Column(scale=2):
            note_mkdwn = gr.Markdown("### 解いた問題を振り返ろう", visible=False)

            understanding = gr.Radio(
                choices=["すべて自力で解けた", "解説を見てわかった", "解説を見てもわからなかった"],
                label="この問題はどの程度理解できましたか？",
                visible=False
            )

            new_checkboxes = gr.CheckboxGroup(
                choices=[],
                label="いま解いた問題について、できたポイントをすべてチェックしよう。復習できましたか？", 
                visible=False, 
                show_label = False
            )

            fluency = gr.Radio(
                choices=["自然だった", "不自然な箇所があった", "全体的に不自然だった"],
                label="この問題の問題文は自然な日本語でしたか？",
                visible=False
            )

            difficulty = gr.Radio(
                choices=["5(難しい)", "4", "3", "2", "1(簡単)"],
                label="この問題はどのくらい難しかったですか？",
                visible=False
            )

            relevance = gr.Radio(
                choices=["5(関連していた)", "4", "3", "2", "1(関連していなかった)"],
                label="この問題はもとの問題とどのくらい関連していましたか？",
                visible=False
            )

            rating = gr.Radio(
                choices=["5(役に立った)", "4", "3", "2", "1(役に立たなかった)"],
                label="この問題はどのぐらい復習の役に立ちましたか？",
                visible=False
            )

            report_markdown = gr.Markdown(
                "この問題について、感想や意見を自由に書いてみよう（任意）。(模範解答が間違ってるかも？, こうすればより良い問題になる, 別解がある, など...)",
                visible=False
            )

            report_type = gr.Radio(
            choices=["模範解答が間違ってるかも？", "わからない場所がある...", "こうすればより良い問題になる", "別解がある", "その他"],
            label="報告のカテゴリはどれですか？",
            visible=False,
            interactive=True
            )

            report_text = gr.Textbox(
                label="こちらに詳しく記述してください",
                placeholder="",
                visible=False,
                interactive=False,
                lines=5
            )
        understanding_state = gr.State()
        rating_state = gr.State()
        fluency_state = gr.State()
        difficulty_state = gr.State()
        relevance_state = gr.State()
        check_state = gr.State()
        report_type_state = gr.State()
        report_text_state = gr.State()

    with gr.Row():
        with gr.Column(scale=1):
            report_btn = gr.Button(
                interactive=False, 
                value="結果を送信する(まずは問題を振り返ってください！)", 
                variant="secondary",
                visible=False
            )

    vanish_btn.click(
        fn=lambda: (
            gr.update(visible=False),
            gr.update(visible=False),
            "VanishedTitle"
        ),
        inputs=None,
        outputs=[title, vanish_btn, operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, title],
        outputs=None
    )

    def generate_status_msg(count_work, count_review, num_rubrics, school):
        color_map = {
            "まったくわからなかった": "red",
            "解説を見てもわからなかった": "red",
            "すべて自力で解けた": "green",
            "一部解説を見て解いた": "orange",
            "解説を見てわかった": "orange",
            "正解": "green",
            "不正解": "red"
        }

        # 2025夏実証用
        if school=="C126210001533":
            if count_work==0: #BookRollで問題を解かせる
                if num_rubrics > 0:
                    html = f"""
                    <div style="text-align: center;">
                        <span style="font-size: 22px; font-weight: bold;">
                        あなたが解いたデータが見つかりませんでした。<br>
                        </span>
                        <span style="font-weight: bold; color: #2196f3;"> <h2>まずはBookRollで、該当の問題を解きましょう！</h2></span><br>
                        <span style="font-weight: bold; color: #2196f3;"> <h3>BookRollで解かないと、下のボタンが有効になりません。<br>BookRollで解いてから、システムに入り直してください。</h3></span><br>
                    </div>
                    """
                    return html
                else:
                    html = f"""
                    <div style="text-align: center;">
                        <span style="font-size: 22px; font-weight: bold;">
                        あなたが解いたデータが見つかりませんでした。<br>
                        </span>
                        <span style="font-weight: bold; color: #2196f3;"> <h2>次は、きちんと振り返りを行ってください。</h2></span><br>
                        <span style="font-weight: bold; color: #2196f3;"> <h3>問題を復習しましょう！</h3></span><br>
                    </div>
                    """
                    return html
        if count_work + count_review > 0:
            if num_rubrics > 0:
                html = f"""
                <div style="text-align: center;">
                    <span style="font-size: 28px; font-weight: bold;">
                    あなたはこの問題を <span style="color: #00c853;">{count_work}回</span> 解き、<br>
                    <span style="color: #00c853;">{count_review}問 </span> 類題を作って解きました。 <br>
                    </span>
                    <span style="font-weight: bold; color: #2196f3;"> <h3>まずはセルフチェックをしましょう！</h3></span><br>
                    右の項目から、自分が理解している部分にチェックを入れましょう。<br>
                    ＊わからないところだらけなら「上記の項目について、ひとつも理解できなかった」を選ぼう。<br>
                    ＊全部わかっていたら全部のポイントにチェックを入れてみよう。より難しい問題が生成されます。
                </div>
                """
            else:
                html = f"""
                <div style="text-align: center;">
                    <span style="font-size: 28px; font-weight: bold;">
                    あなたはこの問題を <span style="color: #00c853;">{count_work}回</span> 解きました。 <br>
                    </span>
                    <span style="font-weight: bold; color: #2196f3;"> <h3>問題を復習しましょう！</h3></span><br>
                </div>
                """
        else:
            if num_rubrics > 0:
                html = f"""
                <div style="text-align: center;">
                    <span style="font-size: 22px; font-weight: bold;">
                    あなたが解いたデータが見つかりませんでした。<br>
                    </span>
                    <span style="font-weight: bold; color: #2196f3;"> <h3>まずはセルフチェックをしましょう！</h3></span><br>
                    右の項目から、自分が理解している部分にチェックを入れましょう。<br>
                    ＊わからないところだらけなら「上記の項目について、ひとつも理解できなかった」を選ぼう。<br>
                    ＊全部わかっていたら全部のポイントにチェックを入れてみよう。より難しい問題が生成されます。
                </div>
                """
            else:
                html = f"""
                <div style="text-align: center;">
                    <span style="font-size: 22px; font-weight: bold;">
                    あなたが解いたデータが見つかりませんでした。<br>
                    </span>
                    <span style="font-weight: bold; color: #2196f3;"> <h3>問題を復習しましょう！</h3></span><br>
                </div>
                """
        return html

    def update_when_dropdown(quiz_title, quiz_text_dict, user, lti):
        if quiz_title:
            quiz_text, contents_id, page, no, school = quiz_text_dict[quiz_title]
            rubric_explanations = get_main_explanations(quiz_title, quiz_text_dict)
            count_work, count_review = get_result_from_db(school, contents_id, page, no, user, lti)
            msg = generate_status_msg(count_work, count_review, len(rubric_explanations), lti["school_id"])
        
            # 2025夏実証用
            if lti["school_id"]=="C126210001533":
                if count_work==0:
                    return (
                        gr.update(value=f'<div style="text-align: center;"><h1> あなたが選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;"> \n{quiz_text} </div>', visible=True),
                        gr.update(choices=rubric_explanations, value=[], visible=True, interactive=False, show_label=False, label="できたポイントをチェックしよう！"),
                        gr.update(visible=True, value=msg),
                        quiz_text,
                        "SelectedExercise",
                        rubric_explanations,
                        contents_id,
                        page,
                        no,
                        count_work,
                        count_review
                    )
                elif count_review==0:
                    if len(rubric_explanations) > 0:
                        return (
                            gr.update(value=f'<div style="text-align: center;"><h1> あなたが選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;"> \n{quiz_text} </div>', visible=True),
                            gr.update(choices=rubric_explanations, value=[], visible=True, interactive=True, label="できたポイントをチェックしよう！", show_label=True),
                            gr.update(visible=True, value=msg),
                            quiz_text,
                            "SelectedExercise",
                            rubric_explanations,
                            contents_id,
                            page,
                            no,
                            count_work,
                            count_review
                        )

            if len(rubric_explanations) > 0:
                return (
                    gr.update(value=f'<div style="text-align: center;"><h1> あなたが選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;"> \n{quiz_text} </div>', visible=True),
                    gr.update(choices=rubric_explanations, value=[], visible=True, interactive=True, show_label=True, label="できたポイントをチェックしよう！"),
                    gr.update(visible=True, value=msg),
                    quiz_text,
                    "SelectedExercise",
                    rubric_explanations,
                    contents_id,
                    page,
                    no,
                    count_work,
                    count_review
                )
            else:
                return (
                    gr.update(value=f'<div style="text-align: center;"><h1> あなたが選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;"> \n{quiz_text} </div>', visible=True),
                    gr.update(choices=rubric_explanations, value=[], visible=True, interactive=True, label="この問題には解答のポイントがついていません。", show_label=True),
                    gr.update(visible=True, value=msg),
                    quiz_text,
                    "SelectedExercise",
                    rubric_explanations,
                    contents_id,
                    page,
                    no,
                    count_work,
                    count_review
                )
        else:
            return (
                gr.update(),
                gr.update(),
                gr.update(),
                "",
                "SelectedExercise",
                [],
                "",
                "",
                "",
                0,
                0
            )

    def open_when_no_rubrics(quiz_title, all_items):
        if quiz_title:
            if (len(all_items) == 0):
                return (
                    gr.update(interactive=False, variant="stop", value="(解答のポイントがない問題は類題をつくれません)"),
                    gr.update(interactive=True, variant="stop", value="そのまま解く")
                )
            else:
                return (
                    gr.update(interactive=False, variant="stop", value="類題をつくる(できたポイントをチェックしてください)"),
                    gr.update(interactive=False, variant="stop", value="そのまま解く(できたポイントをチェックしてください)")
                )
        else:
            return (
                gr.update(),
                gr.update()
            )

    quiz_dropdown.change(
        fn=update_when_dropdown,
        inputs=[quiz_dropdown, quiz_map_state, user_state, lti_state],
        outputs=[
            quiz_text_display, 
            checkboxes,
            status_msg,
            dropdown_state,
            operationname_state,
            checkbox_all_items_state,
            contentsid_state,
            page_state,
            no_state,
            cnt_work_state,
            cnt_review_state
        ]
    ).then(
        fn=open_when_no_rubrics,
        inputs=[quiz_dropdown, checkbox_all_items_state],
        outputs=[gen_quiz_btn, rev_quiz_btn]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, dropdown_state],
        outputs=None
    )

    def update_when_checkboxes(all_items, selected, current_status):
        #checkboxのチェックの変更
        current_select = list(set(selected) ^ set(current_status))
        # 上記の項目について、ひとつも理解できなかった を選択した
        if len(selected) > len(current_status):
            if current_select[0] == "上記の項目について、ひとつも理解できなかった":
                updated_status = ["上記の項目について、ひとつも理解できなかった"]
            else:
                updated_status = [x for x in selected if x != "上記の項目について、ひとつも理解できなかった"]
        else:
            updated_status = selected
        
        # 選択結果を "o" / "x" で辞書化
        result = {
            choice: "o" if choice in updated_status else "x"
            for choice in all_items
        }

        return (
            result, 
            updated_status, 
            gr.update(value=updated_status)
        )
    
    def update_genquizbtn_when_checkboxes(selected, lti, count_work, count_review, all_items):
        if len(selected) == 0:
            if len(all_items) > 0:
                return (
                    gr.update(interactive=False, variant="stop", value="類題をつくる(できたポイントをチェックしてください)"),
                    gr.update(interactive=False, variant="stop", value="そのまま解く(できたポイントをチェックしてください)"),
                    gr.update()
                )
            else:
                return (
                    gr.update(interactive=False, variant="stop", value="(解答のポイントがない問題は類題をつくれません)"),
                    gr.update(interactive=True, variant="stop", value="そのまま解く"),
                    gr.update()
                )
            
        if lti["school_id"]=="C126210001533":
            if count_review == 0:
                return (
                    gr.update(interactive=True, variant="stop", value="類題をつくる"),
                    gr.update(interactive=False, variant="stop", value="そのまま解く(類題を解くと選べるようになります)"),
                    gr.update(interactive=False)
                )
        
        return (
            gr.update(interactive=True, variant="stop", value="類題をつくる"),
            gr.update(interactive=True, variant="stop", value="そのまま解く"),
            gr.update(interactive=False)
        )
    
    checkboxes.change(
        fn=update_when_checkboxes,
        inputs=[checkbox_all_items_state, checkboxes, current_checkbox_state],
        outputs=[checkbox_state, current_checkbox_state, checkboxes]
    ).then(
        fn=update_genquizbtn_when_checkboxes,
        inputs=[checkboxes, lti_state, cnt_work_state, cnt_review_state, checkbox_all_items_state],
        outputs=[gen_quiz_btn, rev_quiz_btn, quiz_dropdown]
    ).then(
        fn=lambda: (
        "SelectedRubricStatus"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    )

    def update_when_gen_quiz_btn(quiz_title, selections, quiz_text_dict, model, lti, num_review):
        rubrics = get_main_explanations(quiz_title, quiz_text_dict)
        rubrics = rubrics[:-1]
        selected = selections or []
        tags = ['_o_' if item in selected else '_x_' for item in rubrics]
        review_point = "この問題は、元の問題の応用問題として、どのくらい役に立ちましたか(どのくらい他の人にオススメしたいですか)？"
        for i in range(len(tags)):
            if tags[i] == "_x_":
                review_point = "この問題は、もとの問題の理解できていなかったポイント「{}」を復習する問題として、どのくらい役に立ちましたか(どのくらい他の人にオススメしたいですか)？".format(rubrics[i])
                break
        quiz_text, contents_id, page, no, school = quiz_text_dict[quiz_title]
        exercise_info = exercise_col.find_one({"school_id": school, "contents_id": contents_id, "page": page, "no": no})
        standard_answer = exercise_info.get("standard_answer", "")
        additional_explanation = exercise_info.get("figure_explanation", "")

        # reason[bittype], ans, f"{elapsed_time_creation:.2f}", prompt
        reason, new_exercise, exercise_creation_time, prompt_exercise = execute0006_ks(quiz_text, standard_answer, rubrics, tags, model, additional_explanation, find_grade(lti["context_title"]))

        tags_for_saving = [True if item in selected else False for item in rubrics]

        return (
            new_exercise,
            exercise_creation_time,
            gr.update(value=reason + "\n" + new_exercise + f"\n問題生成時間:" + exercise_creation_time + "秒" + "\n #### 右側の入力欄に解答の過程を入力するか、紙に解いて答えを出した後、模範解答を見て確認しましょう。\n### 注意：AIの生成問題には誤りを含むことがあります。"), 
            gr.update(visible=True, variant="secondary", interactive=False, value="(問題の解答を作成中...)"),
            gr.update(placeholder="ここに記述してください", visible=True, interactive=True, lines=10),
            tags_for_saving,
            school,
            gr.update(label=review_point),
            prompt_exercise
        )
    
    def update_when_gen_quiz_btn_2(quiz_title, selections, quiz_text_dict, model, lti, num_review, new_exercise):
        rubrics = get_main_explanations(quiz_title, quiz_text_dict)
        rubrics = rubrics[:-1]

        # check_if_solvable(question, knowledge, num, model, grade)
        new_answer, prompt_answer, elapsed_time_solve = check_if_solvable(new_exercise, rubrics, "type1", model, find_grade(lti["context_title"]))

        return (
            new_answer,
            elapsed_time_solve, 
            gr.update(visible=True, variant="primary", interactive=True, value="模範解答を表示"),
            prompt_answer
        )

    gen_quiz_btn.click(
        fn=lambda: (
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(visible=True, variant="secondary", interactive=False, value="(あなたの理解に最適な問題を作成中...)"),
        "SubmittedCheck",
        1
        ),
        inputs=None,
        outputs=[vanish_btn, quiz_dropdown, checkboxes, gen_quiz_btn, rev_quiz_btn, answer_btn, operationname_state, gen_state]
    ).then(
        fn=update_when_gen_quiz_btn,
        inputs=[quiz_dropdown, checkboxes, quiz_map_state, model_state, lti_state, cnt_review_state],
        outputs=[exercise_state, 
                 exercise_creation_time_state,
                 exercise_output, 
                 answer_btn, 
                 student_answer,
                 tags_state,
                 school_state,
                 rating,
                 prompt_exercise_state]
    ).then(
        fn=update_when_gen_quiz_btn_2,
        inputs=[quiz_dropdown, checkboxes, quiz_map_state, model_state, lti_state, cnt_review_state, exercise_state],
        outputs=[answer_state,
                 answer_creation_time_state,
                 answer_btn, 
                 prompt_answer_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    )

    def update_when_rev_quiz_btn(quiz_title, quiz_text_dict):
        quiz_text, contents_id, page, no, school = quiz_text_dict[quiz_title]
        exercise_info = exercise_col.find_one({"school_id": school, "contents_id": contents_id, "page": page, "no": no})
        standard_answer = exercise_info.get("standard_answer", "")
        rubrics = get_main_explanations(quiz_title, quiz_text_dict)
        if (len(rubrics) > 0) and ("上記の項目について、ひとつも理解できなかった" not in rubrics):
            rubrics.append("上記の項目について、ひとつも理解できなかった")

        return (
            quiz_text,
            standard_answer,
            gr.update(value=quiz_text + "\n #### 右側の入力欄に解答の過程を入力するか、紙に解いて答えを出した後、模範解答を見て確認しましょう。\n### 注意：AIの生成問題には誤りを含むことがあります。"), 
            gr.update(visible=True, variant="primary", interactive=True, value="模範解答を表示"),
            gr.update(placeholder="ここに記述してください", visible=True, interactive=True, lines=10),
            [],
            school,
            gr.update(choices=rubrics),
            rubrics
        )

    rev_quiz_btn.click(
        fn=lambda: (
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        False,
        "RevSubmittedCheck",
        0
        ),
        inputs=None,
        outputs=[vanish_btn, quiz_dropdown, checkboxes, gen_quiz_btn, rev_quiz_btn, exercise_saving_state, operationname_state, gen_state]
    ).then(
        fn=update_when_rev_quiz_btn,
        inputs=[quiz_dropdown, quiz_map_state],
        outputs=[exercise_state,
                 answer_state, 
                 exercise_output, 
                 answer_btn, 
                 student_answer,
                 tags_state,
                 school_state,
                 new_checkboxes,
                 new_checkbox_all_items_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    )

    def update_when_answer_btn(solver, answer_time):
        if answer_time:
            return solver + f"\n解答生成時間: {answer_time}秒" + "\n### 注意：AIの生成した解答には誤りを含むことがあります。"
        else:
            return solver + "\n### 注意：AIの生成した解答には誤りを含むことがあります。"
    
    def appear_questionnaire_box(is_gen, rubrics):
        if is_gen == 1: #類題を作った場合
            return (
                gr.update(visible=True),
                gr.update(visible=False, interactive=False, show_label=False), #checkbox
                gr.update(visible=True, interactive=True), #understanding
                gr.update(visible=False, interactive=True), #difficulty
                gr.update(visible=False, interactive=True), #fluency
                gr.update(visible=True, interactive=True), #relevance
                gr.update(visible=True, interactive=True), #rating
                gr.update(visible=True, interactive=False),
                gr.update(visible=False, interactive=True), #report_type
                gr.update(visible=True, interactive=True), #report_text
                gr.update(visible=True),
                gr.update(visible=True, interactive=False),
                gr.update(visible=True, interactive=False),
                gr.update(visible=True),
                0, #understanding
                1, #difficulty
                1, #fluency
                0, #relevance
                0, #rating
                1, #checkbox
                "AnsweredExercise"
            )
        else: #そのまま解いた場合
            if len(rubrics) > 0:
                return (
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=True, show_label=True), # checkbox
                    gr.update(visible=True, interactive=True), #understanding
                    gr.update(visible=False, interactive=True), #difficulty
                    gr.update(visible=False, interactive=False), #fluency
                    gr.update(visible=False, interactive=False), #relevance
                    gr.update(visible=False, interactive=False), #rating
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=False, interactive=True), #report_type
                    gr.update(visible=True, interactive=True), #report_text
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True),
                    0, #understanding
                    1, #difficulty
                    1, #fluency
                    1, #relevance
                    1, #rating
                    0, #checkbox
                    "AnsweredExercise"
                )
            else:
                return (
                    gr.update(visible=True),
                    gr.update(visible=False, interactive=False, show_label=False),
                    gr.update(visible=True, interactive=True), #understanding
                    gr.update(visible=False, interactive=True), #difficulty
                    gr.update(visible=False, interactive=False), #fluency
                    gr.update(visible=False, interactive=False), #relevance
                    gr.update(visible=False, interactive=False), #rating
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=False, interactive=True), #report_type
                    gr.update(visible=True, interactive=True), #report_text
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True),
                    0, #understanding
                    1, #difficulty
                    1, #fluency
                    1, #relevance
                    1, #rating
                    1, #checkbox
                    "AnsweredExercise"
                )

    def get_next_no_for_user(user_name, save, contents_id, page, no):
        if save:
            # 該当ユーザのレコードを全件取得
            records = list(exercise_col.find({
                "contents_id": "ai_generated",
                "page": user_name
            }))

            if len(records)==0:
                return "ai_generated", user_name, "1"  # 該当がなければ1からスタート

            # no を整数に変換して最大値を探す
            max_no = max(int(record.get("no", 0)) for record in records)

            return "ai_generated", user_name, str(max_no + 1)
        else:
            return contents_id, page, no

    answer_btn.click(
        fn=update_when_answer_btn,
        inputs=[answer_state, answer_creation_time_state],
        outputs=answer_output,
    ).then(
        fn=appear_questionnaire_box,
        inputs=[gen_state, new_checkbox_all_items_state],
        outputs=[answer_output,
                 new_checkboxes,
                 understanding,
                 difficulty,
                 fluency,
                 relevance,
                 rating,
                 answer_btn, 
                 report_type, 
                 report_text, 
                 report_markdown, 
                 report_btn, 
                 student_answer, 
                 note_mkdwn, 
                 understanding_state,
                 difficulty_state,
                 fluency_state,
                 relevance_state,
                 rating_state,
                 check_state,
                 operationname_state]
    ).then(
        fn=get_next_no_for_user,
        inputs=[user_state, exercise_saving_state, contentsid_state, page_state, no_state],
        outputs=[new_contentsid_state, new_page_state, new_no_state]
    ).then(
        fn=handle_exercise,
        inputs=[exercise_saving_state, new_no_state, exercise_state, answer_state, user_state, exercise_creation_time_state, answer_creation_time_state, model_state, session_state, lti_state, prompt_exercise_state, prompt_answer_state],
        outputs=None
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, student_answer],
        outputs=None
    )

    def enable_submit(understanding_val, rating_val, difficulty_val, fluency_val, relevance_val, check_val):
        if understanding_val * rating_val * difficulty_val * fluency_val * relevance_val * check_val == 1:
            return gr.update(interactive=True, value="結果を送信する", variant="primary")
        else:
            return gr.update(interactive=False, value="結果を送信する(まずは問題を振り返ってください！)", variant="secondary")
        
    def change_questionnairestate(val):
        if val is not None:
            return 1
        else:
            return 0
        
    def change_box(val):
        if len(val) > 0:
            return 1
        else:
            return 0

    new_checkboxes.change(
        fn=update_when_checkboxes,
        inputs=[new_checkbox_all_items_state, new_checkboxes, new_current_checkbox_state],
        outputs=[new_checkbox_state, new_current_checkbox_state, new_checkboxes]
    ).then(
        fn=change_box,
        inputs=[new_checkboxes],
        outputs=check_state
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: (
        "SelectedNewRubricStatus"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, new_checkbox_state],
        outputs=None
    )

    understanding.change(
        fn=change_questionnairestate,
        inputs=[understanding],
        outputs=[understanding_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedComprehensibility"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, understanding],
        outputs=None
    )

    rating.change(
        fn=change_questionnairestate,
        inputs=[rating],
        outputs=[rating_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedUsefulness"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, rating],
        outputs=None
    )

    difficulty.change(
        fn=change_questionnairestate,
        inputs=[difficulty],
        outputs=[difficulty_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedDifficulty"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, difficulty],
        outputs=None
    )

    fluency.change(
        fn=change_questionnairestate,
        inputs=[fluency],
        outputs=[fluency_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedFluency"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, fluency],
        outputs=None
    )

    relevance.change(
        fn=change_questionnairestate,
        inputs=[relevance],
        outputs=[relevance_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedRelevance"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, relevance],
        outputs=None
    )

    report_type.change(
        fn=lambda _: gr.update(placeholder="例：式が間違っている気がします、〜の部分がわかりません、など。なるべく具体的に", interactive=True, lines=10),
        inputs=report_type,
        outputs=report_text
    ).then(
        fn=lambda: ("SelectedReportType"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, report_type],
        outputs=None
    )

    # 送信ボタンクリックで「送信しました」と表示
    report_btn.click(
        fn=lambda: (gr.update(interactive=False)),
        inputs=None,
        outputs=[report_btn]
    ).then(
        fn=handle_answer,
        inputs=[exercise_saving_state, user_state, session_state, contentsid_state, page_state, no_state, checkbox_state, school_state, new_contentsid_state, new_page_state, new_no_state, student_answer, understanding, rating, difficulty, fluency, relevance, new_checkbox_state, lti_state, report_type, report_text],
        outputs=None
    ).then(
        fn=lambda: (
        "Reported"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, report_type],
        outputs=None
    ).then(
        fn=lambda: (
            gr.update(interactive=True), # vanish_btn
            gr.update(visible=True), # report_result
            gr.update(visible=True, interactive=True, value=None), # quiz_dropdown
            gr.update(visible=False), # quiz_text_display
            gr.update(visible=False), # status_msg
            gr.update(visible=False, show_label=False), # checkboxes
            gr.update(visible=False, show_label=False), # new_checkboxes
            {}, # checkbox_state
            [], # current_checkbox_state
            {}, # new_checkbox_state
            [], # new_current_checkbox_state
            gr.update(visible=True, interactive=False, variant="stop", value="類題をつくる"), # gen_quiz_btn
            gr.update(visible=True, interactive=False, variant="stop", value="そのまま解く"), # rev_quiz_btn
            gr.update(value="復習問題はここに出てきます"), # exercise_output
            gr.update(visible=True, interactive=False, placeholder="(まだ入力できません)", value="", lines=1), # student_answer
            gr.update(visible=False, interactive=False), # answer_btn
            gr.update(visible=False, value=""), # answer_output
            gr.update(visible=False, interactive=False, value=None), # understanding
            gr.update(visible=False, interactive=False, value=None), # difficulty
            gr.update(visible=False, interactive=False, value=None), # fluency
            gr.update(visible=False, interactive=False, value=None), # relevance
            gr.update(visible=False, interactive=False, value=None), # rating
            gr.update(visible=False), # report_markdown
            gr.update(visible=False, interactive=False, value=None), # report_type
            gr.update(visible=False, interactive=False, placeholder="", value="", lines=5), # report_text
            gr.update(visible=False, interactive=False, value="結果を送信する(まずは問題を振り返ってください！)", variant="secondary"), # report_btn
            gr.update(value="## " + random.choice(phrases)), # title
            gr.update(visible=False), # note_mkdwn
            gr.update(value=None), # exercise_creation_time_state
            gr.update(value=None), # answer_creation_time_state
            gr.update(value=None), # overall_creation_time_state
            gr.update(value=None), # tags_state
            gr.update(value=None), # school_state
            0, # understanding_state
            0, # difficulty_state
            0, # fluency_state
            0, # relevance_state
            0, # rating_state
            "", # new_contentsid_state, 
            "", # new_page_state, 
            "", # new_no_state,
            "", # contentsid_state, 
            "", # page_state, 
            "", # no_state,
            "", "", # prompt_exercise_state, prompt_answer_state
            True # exercise_saving_state
        ),
        inputs=None,
        outputs=[
            vanish_btn,
            report_result,
            quiz_dropdown,
            quiz_text_display,
            status_msg,
            checkboxes,
            new_checkboxes,
            checkbox_state,
            current_checkbox_state,
            new_checkbox_state,
            new_current_checkbox_state,
            gen_quiz_btn,
            rev_quiz_btn,
            exercise_output,
            student_answer,
            answer_btn,
            answer_output,
            understanding,
            difficulty,
            fluency,
            relevance,
            rating,
            report_markdown,
            report_type,
            report_text,
            report_btn,
            title,
            note_mkdwn,
            exercise_creation_time_state, 
            answer_creation_time_state,
            overall_creation_time_state,
            tags_state,
            school_state,
            understanding_state,
            difficulty_state,
            fluency_state,
            relevance_state,
            rating_state,
            new_contentsid_state, 
            new_page_state, 
            new_no_state,
            contentsid_state, 
            page_state, 
            no_state,
            prompt_exercise_state, prompt_answer_state, 
            exercise_saving_state
        ]
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

    # ✅ 初回自動読み込み（表示直後に1回実行）
    demo.load(
        fn=load_session_info,
        inputs=None,
        outputs=[lti_state, user_state]
    ).then(
        fn=initial_register,
        inputs=None,
        outputs=None
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