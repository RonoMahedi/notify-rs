"""Fake Google OAuth + FCM HTTP v1 server for tests/fcm.sh (stdlib only).

- POST /token: accepts only a JWT-bearer assertion whose RS256 signature verifies against
  the given public key (checked with the openssl CLI) and whose iss/aud/scope/exp are right.
- POST /v1/projects/<project>/messages:send: needs a bearer we issued; behaves by token prefix:
    dead-*    -> 404 UNREGISTERED          badtok-* -> 400 INVALID_ARGUMENT (bad token)
    busy-*    -> 503 UNAVAILABLE           revoke-* -> 401 once (revokes that bearer), then 200
    anything else -> 200
- GET /stats: counters. Every send is appended to the log file as one JSON line.

usage: fake_fcm.py PORT PUBLIC_KEY_PEM LOG_FILE PROJECT_ID CLIENT_EMAIL
"""
import base64
import json
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT, PUB, LOG, PROJECT, CLIENT = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
lock = threading.Lock()
state = {"token_requests": 0, "sends": 0, "bad_jwt": 0, "issued": set(), "revoked": set(), "revoke_seen": set()}


def b64d(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def jwt_problem(jwt, aud):
    try:
        h, p, sig = jwt.split(".")
        header, claims = json.loads(b64d(h)), json.loads(b64d(p))
    except Exception:
        return "malformed"
    if header.get("alg") != "RS256":
        return "alg"
    with tempfile.NamedTemporaryFile() as data, tempfile.NamedTemporaryFile() as s:
        data.write(f"{h}.{p}".encode()); data.flush()
        s.write(b64d(sig)); s.flush()
        r = subprocess.run(["openssl", "dgst", "-sha256", "-verify", PUB, "-signature", s.name, data.name], capture_output=True)
        if r.returncode != 0:
            return "signature"
    if claims.get("iss") != CLIENT:
        return "iss"
    if claims.get("aud") != aud:
        return f"aud {claims.get('aud')} != {aud}"
    if claims.get("scope") != SCOPE:
        return "scope"
    now = time.time()
    if not (claims.get("iat", 0) <= now + 60 and claims.get("exp", 0) > now):
        return "time"
    return None


def fcm_error(code, status, message, error_code=None):
    err = {"code": code, "message": message, "status": status}
    if error_code:
        err["details"] = [{"@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": error_code}]
    return {"error": err}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/stats":
            with lock:
                return self.reply(200, {k: state[k] for k in ("token_requests", "sends", "bad_jwt")})
        self.reply(404, {})

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        if self.path == "/token":
            form = urllib.parse.parse_qs(body.decode())
            if form.get("grant_type", [""])[0] != "urn:ietf:params:oauth:grant-type:jwt-bearer":
                return self.reply(400, {"error": "unsupported_grant_type"})
            problem = jwt_problem(form.get("assertion", [""])[0], f"http://{self.headers['Host']}/token")
            with lock:
                if problem:
                    state["bad_jwt"] += 1
                    return self.reply(400, {"error": "invalid_grant", "error_description": f"bad assertion: {problem}"})
                state["token_requests"] += 1
                tok = f"fake-access-{state['token_requests']}"
                state["issued"].add(tok)
            return self.reply(200, {"access_token": tok, "expires_in": 3600, "token_type": "Bearer"})

        if self.path == f"/v1/projects/{PROJECT}/messages:send":
            bearer = self.headers.get("Authorization", "").removeprefix("Bearer ")
            msg = json.loads(body)
            token = msg["message"]["token"]
            with lock:
                state["sends"] += 1
                if bearer not in state["issued"] or bearer in state["revoked"]:
                    code, resp = 401, fcm_error(401, "UNAUTHENTICATED", "Request had invalid authentication credentials.")
                elif token.startswith("revoke-") and token not in state["revoke_seen"]:
                    state["revoke_seen"].add(token)
                    state["revoked"].add(bearer)
                    code, resp = 401, fcm_error(401, "UNAUTHENTICATED", "Request had invalid authentication credentials.")
                elif token.startswith("dead-"):
                    code, resp = 404, fcm_error(404, "NOT_FOUND", "Requested entity was not found.", "UNREGISTERED")
                elif token.startswith("badtok-"):
                    code, resp = 400, fcm_error(400, "INVALID_ARGUMENT", "The registration token is not a valid FCM registration token", "INVALID_ARGUMENT")
                elif token.startswith("busy-"):
                    code, resp = 503, fcm_error(503, "UNAVAILABLE", "The service is currently unavailable.", "UNAVAILABLE")
                else:
                    code, resp = 200, {"name": f"projects/{PROJECT}/messages/{state['sends']}"}
                with open(LOG, "a") as f:
                    f.write(json.dumps({"token": token, "code": code, "body": msg}) + "\n")
            return self.reply(code, resp)
        self.reply(404, {})


ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
