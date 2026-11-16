#!/bin/bash
# Security tests for email + password sign-in. Needs the stack running.
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
C=http://localhost:8090; H='content-type: application/json'; KEY="${ADMIN_TOKEN:-dev-admin-token-change-me}"
T=$(date +%s); A="auth-admin-$T@example.com"; APW='Admin-Pass-123456'; M="auth-marketer-$T@example.com"
J=$(mktemp -d)
# Never leave test accounts (with passwords written in this file) in the database.
cleanup(){ rm -rf "$J"; docker compose exec -T postgres psql -U notify -d notify -qc "DELETE FROM admin_users WHERE email IN ('$A', '$M')" >/dev/null 2>&1; }
trap cleanup EXIT
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
DB(){ docker compose exec -T postgres psql -U notify -d notify -tAc "$1" | tr -d '[:space:]'; }
login(){ local b="{\"email\":\"$1\",\"password\":\"$2\"}"; curl -s -o "$J/body" -w '%{http_code}' -c "$J/$3" -H "$H" "$C/v1/auth/login" -d "$b"; }
code(){ local jar=$1; shift; curl -s -o /dev/null -w '%{http_code}' -b "$J/$jar" "$@"; }
tok(){ awk '$6=="notify_session"{print $7}' "$J/$1"; }

echo "== account creation (CLI) + storage"
OUT=$(docker compose exec -T -e NEW_PASSWORD="$APW" campaign campaign create-admin "$A" admin 2>&1)
check "CLI creates admin" "$(echo "$OUT" | grep -c "Created admin $A")" 1
check "CLI refuses a duplicate (non-zero exit, still one row)" "$(docker compose exec -T -e NEW_PASSWORD="$APW" campaign campaign create-admin "$A" >/dev/null 2>&1 && echo created || echo refused)/$(DB "select count(*) from admin_users where email='$A'")" "refused/1"
check "password stored as Argon2id hash" "$(DB "select password_hash like '\$argon2id\$%' from admin_users where email='$A'")" t
check "plain password not stored anywhere" "$(DB "select count(*) from admin_users where password_hash like '%$APW%'")" 0

echo "== sign-in"
B='{"email":"'$A'","password":"wrong-password-1"}'
WRONG=$(curl -s -H "$H" $C/v1/auth/login -d "$B")
B='{"email":"nobody-'$T'@example.com","password":"wrong-password-1"}'
NOUSER=$(curl -s -H "$H" $C/v1/auth/login -d "$B")
check "wrong password -> generic message" "$WRONG" '{"error":"Wrong email or password"}'
check "unknown email -> identical message (no user enumeration)" "$NOUSER" "$WRONG"
check "correct password -> 200" "$(login "$A" "$APW" admin)" 200
check "email is case-insensitive" "$(login "$(echo "$A" | tr a-z A-Z)" "$APW" admin2)" 200
SC=$(curl -s -D - -o /dev/null -H "$H" $C/v1/auth/login -d "{\"email\":\"$A\",\"password\":\"$APW\"}" | grep -i '^set-cookie')
check "cookie is HttpOnly" "$(echo "$SC" | grep -c 'HttpOnly')" 1
check "cookie is SameSite=Strict" "$(echo "$SC" | grep -c 'SameSite=Strict')" 1
check "cookie expires in 12h" "$(echo "$SC" | grep -c 'Max-Age=43200')" 1
check "DB stores only SHA-256 of session token" "$(DB "select count(*) from admin_sessions where token_hash = sha256('$(tok admin)'::bytea)")/$(DB "select count(*) from admin_sessions where encode(token_hash,'escape') = '$(tok admin)'")" "1/0"
check "/auth/me returns the user" "$(curl -s -b "$J/admin" $C/v1/auth/me | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["email"],d["role"])')" "$A admin"
check "API with session cookie -> 200" "$(code admin $C/v1/segments)" 200
check "API without cookie -> 401" "$(curl -s -o /dev/null -w '%{http_code}' $C/v1/segments)" 401
check "tampered cookie -> 401" "$(curl -s -o /dev/null -w '%{http_code}' -H "Cookie: notify_session=$(printf 'f%.0s' $(seq 1 64))" $C/v1/segments)" 401

echo "== cross-site request forgery"
B='{"rule":{"field":"country","op":"eq","value":"BD"}}'
check "same-origin POST allowed" "$(code admin -H "$H" -H 'Origin: http://localhost:8090' $C/v1/segments/preview -d "$B")" 200
check "cross-origin POST blocked" "$(code admin -H "$H" -H 'Origin: http://evil.example' $C/v1/segments/preview -d "$B")" 403
check "look-alike origin blocked" "$(code admin -H "$H" -H 'Origin: http://localhost:8090.evil.example' $C/v1/segments/preview -d "$B")" 403
B="{\"email\":\"$A\",\"password\":\"$APW\"}"
check "cross-origin login blocked (login CSRF)" "$(curl -s -o /dev/null -w '%{http_code}' -H "$H" -H 'Origin: http://evil.example' $C/v1/auth/login -d "$B")" 403

echo "== roles + user management"
B="{\"email\":\"$M\",\"name\":\"Test Marketer\",\"role\":\"marketer\",\"password\":\"short\"}"
check "weak password rejected" "$(code admin -H "$H" $C/v1/users -d "$B")" 400
B="{\"email\":\"$M\",\"name\":\"Test Marketer\",\"role\":\"marketer\",\"password\":\"Marketer-Pass-1\"}"
check "admin creates marketer" "$(code admin -H "$H" $C/v1/users -d "$B")" 201
check "duplicate email -> 409" "$(code admin -H "$H" $C/v1/users -d "$B")" 409
MID=$(DB "select id from admin_users where email='$M'"); AID=$(DB "select id from admin_users where email='$A'")
check "marketer signs in" "$(login "$M" 'Marketer-Pass-1' m1)" 200
check "marketer can use campaigns API" "$(code m1 $C/v1/campaigns)" 200
check "marketer can't list users" "$(code m1 $C/v1/users)" 403
check "marketer can't create users" "$(code m1 -H "$H" $C/v1/users -d '{"email":"x@example.com","role":"admin","password":"Whatever-123"}')" 403
check "admin can't disable self" "$(code admin -X PATCH -H "$H" $C/v1/users/$AID -d '{"disabled":true}')" 400
check "admin can't demote self" "$(code admin -X PATCH -H "$H" $C/v1/users/$AID -d '{"role":"marketer"}')" 400
check "disable marketer" "$(code admin -X PATCH -H "$H" $C/v1/users/$MID -d '{"disabled":true}')" 200
check "  ...their session is revoked at once" "$(code m1 $C/v1/campaigns)" 401
check "  ...and they can't sign in" "$(login "$M" 'Marketer-Pass-1' m1)" 403
check "re-enable + reset password" "$(code admin -X PATCH -H "$H" $C/v1/users/$MID -d '{"disabled":false,"password":"Reset-Pass-4567"}')" 200
check "  ...old password no longer works" "$(login "$M" 'Marketer-Pass-1' m1)" 401
check "  ...new password works" "$(login "$M" 'Reset-Pass-4567' m1)" 200

echo "== change password"
login "$M" 'Reset-Pass-4567' m2 >/dev/null
check "wrong current password -> 400" "$(code m1 -H "$H" $C/v1/auth/password -d '{"current_password":"nope-nope-nope","new_password":"Brand-New-Pass-9"}')" 400
check "change password -> 200" "$(code m1 -H "$H" $C/v1/auth/password -d '{"current_password":"Reset-Pass-4567","new_password":"Brand-New-Pass-9"}')" 200
check "  ...this session stays signed in" "$(code m1 $C/v1/auth/me)" 200
check "  ...other sessions signed out" "$(code m2 $C/v1/auth/me)" 401
check "  ...old password fails, new works" "$(login "$M" 'Reset-Pass-4567' x)/$(login "$M" 'Brand-New-Pass-9' x)" "401/200"

echo "== brute-force lockout"
for i in 1 2 3 4 5; do login "$M" "bad-guess-$i-xx" x >/dev/null; done
check "6th try is locked, even with the right password" "$(login "$M" 'Brand-New-Pass-9' x)" 429
check "  ...message says when to retry" "$(grep -c 'Try again in 15 minutes' "$J/body")" 1
check "CLI reset-password clears the lockout" "$(docker compose exec -T -e NEW_PASSWORD='After-Lock-123' campaign campaign reset-password "$M" 2>&1 | grep -c 'lockout cleared')" 1
check "  ...sign-in works again" "$(login "$M" 'After-Lock-123' x)" 200

echo "== sign out + API key"
check "sign out -> 204" "$(code admin -X POST $C/v1/auth/logout)" 204
check "  ...session no longer valid" "$(code admin $C/v1/auth/me)" 401
check "API key (Bearer) still works for scripts" "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY" $C/v1/segments)" 200
check "wrong API key -> 401" "$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer not-the-key-xxxxxxxx' $C/v1/segments)" 401

echo; echo "AUTH RESULT: $PASS passed, $FAIL failed"
