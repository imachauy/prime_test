#lti
from fastapi import FastAPI, Request, HTTPException, Depends, status, APIRouter
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware # for autocomplete
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_404_NOT_FOUND
import uvicorn
import httpx
from pylti.common import verify_request_common, LTIException
from dotenv import load_dotenv


# gradio_app.pyのASGIアプリをインポート！
from gradio import mount_gradio_app
from gradio_app_dashboard import demo

#general
import os
from dotenv import load_dotenv
import gradio as gr

from fastapi.templating import Jinja2Templates

app = FastAPI() # main app
router = APIRouter()

app.add_middleware( # for autocomplete TODO add API key
        CORSMiddleware,
        allow_origins=["*"],  # Allow all origins (you can restrict it to specific domains)
        allow_credentials=True,
        allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
        allow_headers=["*"],  # Allow all headers
    )

# Load environment variables from .env file
load_dotenv()

app.add_middleware(SessionMiddleware, secret_key=os.environ.get('FASTAPI_SECRET_KEY'))

@app.on_event("startup")
def startup_event():
    print("[46] Server startup: Initializing database")
    app.mount("/prime_dashboard/static", StaticFiles(directory="static"), name="static")

def get_browser_language(request: Request):
    try:
        accept_language = request.headers.get("accept-language", "en")
        languages = accept_language.split(',')
        for lang in languages:
            if lang.strip().lower().startswith('ja'):
                print(f"[55] Browser language: {lang}")
                return 'ja'
            elif lang.strip().lower().startswith('en'):
                print(f"[57] Browser language: {lang}")
                return 'en'
        # If no match found, default to the first language in the list
        primary_language = languages[0].split(';')[0].split('-')[0].lower()
        print(f"[62] Browser language: {primary_language}")
    except Exception as e:
        primary_language = 'en'  # default to English
        print(f"[65] Error getting browser language: {str(e)}")
    return primary_language

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")

LTI_URL = os.getenv("LTI_URL", "https://dev.let.media.kyoto-u.ac.jp/prime_dashboard/lti/login")

css = """
h1 {
    text-align: center;
    display: block;
}
"""
###########################

# Function to load all LTI credentials from environment variables
max_number = os.environ.get('MAX_GENERATION')
def load_lti_credentials():
    consumers = {}
    i = 1
    while True:
        key = os.getenv(f'LTI_CONSUMER_KEY_{i}')
        secret = os.getenv(f'LTI_SHARED_SECRET_{i}')
        if not key or not secret:
            break
        consumers[key] = {"secret": secret}
        i += 1
        if i == max_number:
            break
    return consumers

# Load all LTI consumers at startup
LTI_CONSUMERS = load_lti_credentials()

# LTI Request validation
async def validate_lti_request(request: Request):
    # First, ensure you await the form data from the request
    form_data = await request.form()

    # dictionary of consumers
    consumers = LTI_CONSUMERS
    print("[104] Validating LTI request")
    common_request_verification = False

    try: 
        # Call verify_request_common with all the necessary parameters
        common_request_verification = verify_request_common(
            consumers=consumers,
            url=LTI_URL,#str(request.url),
            method=request.method,
            headers=dict(request.headers),
            params=dict(form_data)  # Ensure this is a dict if not already
        )
        print("[116] LTI request validation successful")

    except LTIException as e:
        print(f"[119] LTI validation failed: {e}")

    return common_request_verification


async def get_current_user(request: Request): # Dependsと関わってくる
    user = request.session.get('user', {})

    if not user:
        print(f"[128] Not authenticated. Request: {request}")
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


####################################### apps in separate dockers

@app.exception_handler(Exception)
async def universal_exception_handler(request: Request, exc: Exception):
    # Log the error if needed
    print(f"[138] Unhandled exception in FastAPI routing: {exc} for {request}")

    # Check the type of exception and handle it accordingly
    if isinstance(exc, HTTPException):
        status_code = exc.status_code
    else:
        status_code = 500  # Default to 500 for all other exceptions
    
    return JSONResponse(
        status_code,
        content={"detail": "サーバー内部でエラーが発生しました"}
    )

# Create a single client for each service to be reused
async def get_httpx_client(base_url: str):
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
        http2=True
    )

@app.on_event("startup")
async def startup_event():
    print("[162] Server startup: Initializing HTML clients")

    # Fetch URLs from environment variables
    prime_dashboard_url = os.getenv("PRIME_DASHBOARD_URL", "http://fastapi-app:7861")

    app.state.prime_dashboard_client = await get_httpx_client(prime_dashboard_url)


@app.on_event("shutdown")
async def shutdown_event():
    print("[172] Server shutdown: Closing HTTP clients")
    await app.state.prime_dashboard_saikyo_client.aclose()
    await app.state.prime_dashboard_dcat_client.aclose()


# Log incoming requests
@app.middleware("http") 
async def log_request(request: Request, call_next):
    response = await call_next(request)
    print(f"[181] Request: {request.method} {request.url} - Response: {response.status_code}")
    return response

async def stream_response(response):
    async for chunk in response.aiter_raw():
        yield chunk

async def proxy_request(client, request: Request, path: str):
    url = httpx.URL(path=path, query=request.url.query.encode("utf-8"))
    
    headers = dict(request.headers)
    headers.pop("host", None)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            rp_req = client.build_request(
                request.method, 
                url,
                headers=headers,
                content=await request.body()
            )
            rp_resp = await client.send(rp_req, stream=True)
            print(f"[204] Proxy request to {url} completed with status {rp_resp.status_code} (attempt {attempt + 1})")
            return StreamingResponse(
                stream_response(rp_resp),
                status_code=rp_resp.status_code,
                headers=dict(rp_resp.headers)
            )
        except (httpx.RequestError, TimeoutError) as e:
            print(f"[211] Attempt {attempt + 1} failed for proxying request to {url}: {e}")      

    # This line should never be reached due to the exception in the loop
    print(f"[214] Max retries reached for proxying request to {url}")
    raise HTTPException(status_code=502, detail="Max retries reached")

@app.api_route("/prime_dashboard/{path:path}") # legacy path
async def proxy_to_prime_dashboard(request: Request, path: str, user: dict = Depends(get_current_user)):
    return await proxy_request(app.state.prime_dashboard_client, request, path)

####### LTI #############################################################################

@app.post('/prime_dashboard/lti/login') #/prime_dashboard/lti/login に「POST」でアクセスされた時だけ、この関数が呼ばれます。
async def lti_launch(request: Request):
    print(f"[225] Validating LTI request. Request headers: {dict(request.headers)}")
    valid = await validate_lti_request(request) #ltiリクエストをpostする
    if not valid:
        return {'error': 'Invalid LTI request'} 

    # Extracting additional fields from the form data
    form_data = await request.form()

    user_id = form_data.get('user_id')
    roles = form_data.get('roles', 'unknown')
    oauth_consumer_key = form_data.get('oauth_consumer_key', 'unknown')
    context_id = form_data.get('context_id', 'unknown')
    context_title = form_data.get('context_title', 'unknown')
    if user_id:

        print(f"[250] LTI launch successful for user {user_id}")

        browser_language=get_browser_language(request)
        school = "unknown"
        if oauth_consumer_key:
            school = oauth_consumer_key

        # Extending user information in session including new fields
        user_info = {
            'user_id': user_id,
            'roles': roles,
            'browser_language': browser_language,
            'oauth_consumer_key': oauth_consumer_key,
            'context_id': context_id,
            'context_title': context_title,
            'school_id': school
        }
        print(f"[286] Successful LTI login: {user_info}")
        request.session['user'] = user_info


        return RedirectResponse(url='/prime_dashboard/ui/', status_code=status.HTTP_303_SEE_OTHER)
    
    print("[292] User ID missing in LTI request")
    return {'error': 'User ID missing in LTI request'}

@router.get("/go-to-gradio")
async def go_to_gradio():
    return RedirectResponse(url="/prime_dashboard/ui/")

app.include_router(router, prefix="/prime_dashboard")

# Gradio Mount（/ui以下はGradio専用にする！）
mount_gradio_app(app, demo, path="/ui/", root_path="/ui")

if __name__ == '__main__':
    uvicorn.run(app, root_path="/prime_dashboard")