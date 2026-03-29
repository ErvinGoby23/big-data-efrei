#!/usr/bin/env bash
# test_api.sh – Test rapide de tous les endpoints
# Usage: ./test_api.sh [http://localhost:8000]

BASE="${1:-http://localhost:8000}"
echo "=== Test API Yelp – $BASE ==="

# ─── Health ────────────────────────────────────────────────────────────────
echo -e "\n[1] Health check"
curl -s "$BASE/health" | python3 -m json.tool

# ─── Login ─────────────────────────────────────────────────────────────────
echo -e "\n[2] Login (admin / admin123)"
RESP=$(curl -s -X POST "$BASE/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123")
echo $RESP | python3 -m json.tool

TOKEN=$(echo $RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo ">> Token: ${TOKEN:0:40}..."

AUTH="-H \"Authorization: Bearer $TOKEN\""

# ─── Me ────────────────────────────────────────────────────────────────────
echo -e "\n[3] /auth/me"
curl -s "$BASE/auth/me" -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── Stats ─────────────────────────────────────────────────────────────────
echo -e "\n[4] /stats"
curl -s "$BASE/stats" -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── DM1 ───────────────────────────────────────────────────────────────────
echo -e "\n[5] DM1 – Top villes (page 1, 5 résultats)"
curl -s "$BASE/datamarts/top-villes?page=1&page_size=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── DM2 ───────────────────────────────────────────────────────────────────
echo -e "\n[6] DM2 – Catégories (page 1)"
curl -s "$BASE/datamarts/categories?page=1&page_size=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── DM3 ───────────────────────────────────────────────────────────────────
echo -e "\n[7] DM3 – Restaurants ouverts à Las Vegas"
curl -s "$BASE/datamarts/restaurants?page=1&page_size=5&city=las+vegas&is_open=1" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── DM4 ───────────────────────────────────────────────────────────────────
echo -e "\n[8] DM4 – Évolution temporelle"
curl -s "$BASE/datamarts/evolution-temporelle?page=1&page_size=6" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

echo -e "\n[9] DM4 – Tendances montantes"
curl -s "$BASE/datamarts/tendances?trend=montant&page_size=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── DM5 ───────────────────────────────────────────────────────────────────
echo -e "\n[10] DM5 – Voix client (excellent)"
curl -s "$BASE/datamarts/voix-client?satisfaction=excellent&page_size=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ─── Sans token ────────────────────────────────────────────────────────────
echo -e "\n[11] Sans token → doit retourner 401"
curl -s -o /dev/null -w "HTTP %{http_code}\n" "$BASE/datamarts/top-villes"

echo -e "\n=== Tests terminés ==="
