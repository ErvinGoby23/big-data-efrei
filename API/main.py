"""
API REST – Yelp Data Warehouse
Sécurisée par JWT, pagination sur tous les datamarts.
"""

import os
import math
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="api.txt",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("api")

# ─── Config ─────────────────────────────────────────────────────────────────
SECRET_KEY   = os.getenv("JWT_SECRET",      "yelp-secret-key-change-in-prod-2024")
ALGORITHM    = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))

PG_HOST = os.getenv("PG_HOST",     "postgres-yelp")
PG_PORT = os.getenv("PG_PORT",     "5432")
PG_DB   = os.getenv("PG_DB",       "yelp_dw")
PG_USER = os.getenv("PG_USER",     "yelp")
PG_PASS = os.getenv("PG_PASSWORD", "yelp123")

# ─── Auth setup ─────────────────────────────────────────────────────────────
# Utilise SHA-256 (stdlib) pour éviter toute dépendance bcrypt au niveau module.
# SHA-256 suffit largement pour un exam – en prod on utiliserait bcrypt/argon2.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def _sha256(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

USERS_DB = {
    "admin":   {"username": "admin",   "hashed_password": _sha256("admin123"),  "role": "admin"},
    "student": {"username": "student", "hashed_password": _sha256("efrei2024"), "role": "reader"},
}

# ─── App ────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Yelp Data Warehouse API",
    description="API REST sécurisée JWT – Data Engineering M1 EFREI",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Pydantic models ────────────────────────────────────────────────────────
class Token(BaseModel):
    access_token: str
    token_type:   str
    expires_in:   int

class TokenData(BaseModel):
    username: Optional[str] = None

class PaginatedResponse(BaseModel):
    page:        int
    page_size:   int
    total_items: int
    total_pages: int
    data:        list

# ─── DB helper ──────────────────────────────────────────────────────────────
def get_db():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DB, user=PG_USER, password=PG_PASS
        )
        return conn
    except Exception as e:
        log.error("DB connection failed: {}".format(e))
        raise HTTPException(status_code=503, detail="Base de données indisponible")

def paginate(table: str, page: int, page_size: int,
             filters: dict = None, order_by: str = None):
    """Requête paginée générique sur une table PostgreSQL."""
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        where_clause = ""
        params = []
        if filters:
            conditions = [f"{k} = %s" for k in filters]
            where_clause = "WHERE " + " AND ".join(conditions)
            params = list(filters.values())

        order_clause = f"ORDER BY {order_by}" if order_by else ""

        # Count total
        cur.execute(f"SELECT COUNT(*) FROM {table} {where_clause}", params)
        total = cur.fetchone()["count"]

        # Fetch page
        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT * FROM {table} {where_clause} {order_clause} LIMIT %s OFFSET %s",
            params + [page_size, offset]
        )
        rows = cur.fetchall()

        return {
            "page":        page,
            "page_size":   page_size,
            "total_items": total,
            "total_pages": math.ceil(total / page_size) if total > 0 else 0,
            "data":        [dict(r) for r in rows]
        }
    except Exception as e:
        conn.rollback()
        log.error("Erreur paginate table={} : {}".format(table, str(e)))
        # Message lisible au lieu de Internal Server Error
        raise HTTPException(
            status_code=500,
            detail="Erreur base de données : {} — La table '{}' existe-t-elle ? (datamart.py exécuté ?)".format(str(e), table)
        )
    finally:
        conn.close()

# ─── JWT helpers ────────────────────────────────────────────────────────────
def verify_password(plain: str, hashed: str) -> bool:
    # Comparaison timing-safe pour éviter les timing attacks
    return secrets.compare_digest(_sha256(plain), hashed)

def authenticate_user(username: str, password: str):
    user = USERS_DB.get(username)
    if not user or not verify_password(password, user["hashed_password"]):
        return None
    return user

def create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = USERS_DB.get(username)
    if user is None:
        raise credentials_exception
    log.info("Requête authentifiée – user={}".format(username))
    return user

# ────────────────────────────────────────────────────────────────────────────
# ROUTES
# ────────────────────────────────────────────────────────────────────────────

# ─── Auth ───────────────────────────────────────────────────────────────────
@app.post("/auth/login", response_model=Token, tags=["Auth"])
async def login(form: OAuth2PasswordRequestForm = Depends()):
    """
    Authentification – retourne un Bearer JWT.

    **Comptes de test :**
    - `admin` / `admin123`
    - `student` / `efrei2024`
    """
    user = authenticate_user(form.username, form.password)
    if not user:
        log.error("Échec auth – username={}".format(form.username))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Identifiants incorrects",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token({"sub": user["username"], "role": user["role"]})
    log.info("Login OK – username={}".format(user["username"]))
    return {"access_token": token, "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60}

@app.get("/auth/me", tags=["Auth"])
async def me(current_user=Depends(get_current_user)):
    """Retourne les infos de l'utilisateur connecté."""
    return {"username": current_user["username"], "role": current_user["role"]}

# ─── Health ─────────────────────────────────────────────────────────────────
@app.get("/health", tags=["Système"])
async def health():
    """Vérification que l'API et la base sont accessibles."""
    try:
        conn = get_db()
        conn.close()
        db_status = "ok"
    except Exception as e:
        db_status = str(e)
    return {"status": "ok", "db": db_status, "timestamp": datetime.utcnow().isoformat()}

@app.get("/debug/db", tags=["Système"])
async def debug_db():
    """
    Diagnostic : liste les tables présentes dans yelp_dw et teste la connexion.
    (Sans auth pour faciliter le debug)
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DB, user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT tablename, pg_size_pretty(pg_relation_size(quote_ident(tablename))) AS size
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        tables = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {
            "connection": "ok",
            "host": PG_HOST,
            "db": PG_DB,
            "tables": tables,
            "tip": "Si dm1_top_villes est absent → lancer datamart.py via spark-submit"
        }
    except Exception as e:
        return {"connection": "error", "detail": str(e),
                "host": PG_HOST, "db": PG_DB}

# ─── DM1 – Top villes ───────────────────────────────────────────────────────
@app.get("/datamarts/top-villes", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm1_top_villes(
    page:      int = Query(1,   ge=1,  description="Numéro de page"),
    page_size: int = Query(10,  ge=1,  le=100, description="Taille de page (max 100)"),
    state:     Optional[str] = Query(None, description="Filtrer par état (ex: NV, AZ)"),
    current_user=Depends(get_current_user)
):
    """
    **DM1 – Top villes par attractivité.**

    Score = `avg_stars × 0.4 + log(total_avis) × 0.4 + log(avg_checkins) × 0.2`
    """
    filters = {"state": state.upper()} if state else None
    result  = paginate("dm1_top_villes", page, page_size,
                       filters=filters, order_by="score_attractivite DESC")
    log.info("DM1 consulté – page={} user={}".format(page, current_user["username"]))
    return result

# ─── DM2 – Catégories ───────────────────────────────────────────────────────
@app.get("/datamarts/categories", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm2_categories(
    page:      int = Query(1,  ge=1, description="Numéro de page"),
    page_size: int = Query(10, ge=1, le=100),
    current_user=Depends(get_current_user)
):
    """
    **DM2 – Performance par catégorie de restaurant.**

    Agrégation : note moyenne, nombre de restaurants, review_count moyen.
    Filtré sur les catégories avec au moins 10 restaurants.
    """
    result = paginate("dm2_performance_concepts", page, page_size,
                      order_by="avg_stars DESC")
    log.info("DM2 consulté – page={} user={}".format(page, current_user["username"]))
    return result

# ─── DM3 – Features restaurants ─────────────────────────────────────────────
@app.get("/datamarts/restaurants", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm3_restaurants(
    page:      int = Query(1,  ge=1, description="Numéro de page"),
    page_size: int = Query(10, ge=1, le=100),
    city:      Optional[str] = Query(None, description="Filtrer par ville"),
    state:     Optional[str] = Query(None, description="Filtrer par état"),
    is_open:   Optional[int] = Query(None, description="1 = ouvert, 0 = fermé"),
    current_user=Depends(get_current_user)
):
    """
    **DM3 – Features par restaurant** (utile pour un modèle de prédiction).

    Contient : nb_avis, avg_review_stars, avg_useful, avg_funny, avg_cool, checkin_count.
    """
    filters = {}
    if city:    filters["city"]    = city.lower()
    if state:   filters["state"]   = state.upper()
    if is_open is not None: filters["is_open"] = is_open
    result = paginate("dm3_features_prediction", page, page_size,
                      filters=filters or None, order_by="nb_avis DESC")
    log.info("DM3 consulté – page={} user={}".format(page, current_user["username"]))
    return result

# ─── DM4 – Évolution temporelle ─────────────────────────────────────────────
@app.get("/datamarts/evolution-temporelle", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm4_evolution(
    page:      int = Query(1,  ge=1, description="Numéro de page"),
    page_size: int = Query(24, ge=1, le=100),
    current_user=Depends(get_current_user)
):
    """
    **DM4 – Évolution mensuelle du volume d'avis et de la note moyenne.**

    Granularité : année / mois.
    """
    result = paginate("dm4_evolution_temporelle", page, page_size,
                      order_by="year ASC, month ASC")
    log.info("DM4 évolution consulté – page={} user={}".format(page, current_user["username"]))
    return result

@app.get("/datamarts/tendances", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm4_tendances(
    page:      int = Query(1,  ge=1, description="Numéro de page"),
    page_size: int = Query(10, ge=1, le=100),
    trend:     Optional[str] = Query(None, description="montant | stable | declinant"),
    current_user=Depends(get_current_user)
):
    """
    **DM4 – Tendances par restaurant** (montant / stable / déclinant).

    Comparaison de la note sur 6 mois glissants.
    """
    filters = {"trend": trend} if trend else None
    result  = paginate("dm4_tendances_business", page, page_size,
                       filters=filters, order_by="year DESC, month DESC")
    log.info("DM4 tendances consulté – page={} user={}".format(page, current_user["username"]))
    return result

# ─── DM5 – Voix du client ───────────────────────────────────────────────────
@app.get("/datamarts/voix-client", response_model=PaginatedResponse, tags=["Datamarts"])
async def dm5_voix_client(
    page:      int = Query(1,  ge=1, description="Numéro de page"),
    page_size: int = Query(10, ge=1, le=100),
    satisfaction: Optional[str] = Query(
        None,
        description="excellent | bon | moyen | mauvais"
    ),
    current_user=Depends(get_current_user)
):
    """
    **DM5 – Voix du client** : utilité des avis, taux de satisfaction.

    Label : `excellent` (≥4.5), `bon` (≥3.5), `moyen` (≥2.5), `mauvais` (<2.5).
    """
    filters = {"satisfaction_label": satisfaction} if satisfaction else None
    result  = paginate("dm5_voix_client", page, page_size,
                       filters=filters, order_by="avg_useful DESC")
    log.info("DM5 consulté – page={} user={}".format(page, current_user["username"]))
    return result

# ─── Stats globales (bonus) ──────────────────────────────────────────────────
@app.get("/stats", tags=["Système"])
async def stats(current_user=Depends(get_current_user)):
    """Résumé rapide du contenu de chaque datamart."""
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        tables = [
            "dm1_top_villes",
            "dm2_performance_concepts",
            "dm3_features_prediction",
            "dm4_evolution_temporelle",
            "dm4_tendances_business",
            "dm5_voix_client"
        ]
        result = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                result[t] = cur.fetchone()["count"]
            except Exception:
                result[t] = "table absente"
        return result
    finally:
        conn.close()
