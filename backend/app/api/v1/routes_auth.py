from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import create_access_token, get_current_user, hash_password, verify_password
from app.db.models import User
from app.db.session import get_db
from app.services.i18n_service import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, normalize_preferred_language


router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterBody(BaseModel):
    nickname: str = Field(..., min_length=3, max_length=64)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=200)
    preferred_language: str = Field(default=DEFAULT_LANGUAGE, min_length=2, max_length=2)


class LoginBody(BaseModel):
    nickname: str = Field(..., min_length=3, max_length=64)
    password: str = Field(..., min_length=1, max_length=200)


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: int
    nickname: str
    email: EmailStr | None = None
    preferred_language: str = DEFAULT_LANGUAGE


class UpdateLanguageBody(BaseModel):
    preferred_language: str = Field(..., min_length=2, max_length=2)


@router.post("/register", response_model=UserOut)
async def register(body: RegisterBody, db: AsyncSession = Depends(get_db)) -> UserOut:
    nickname = body.nickname.strip()
    if not nickname or " " in nickname:
        raise HTTPException(status_code=400, detail="nickname must not contain spaces")
    exists_n = (await db.execute(select(User).where(User.nickname == nickname))).scalar_one_or_none()
    if exists_n:
        raise HTTPException(status_code=409, detail="nickname already taken")

    email = body.email.strip().lower()
    exists_e = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if exists_e:
        raise HTTPException(status_code=409, detail="email already registered")

    preferred_language = normalize_preferred_language(body.preferred_language)
    if preferred_language not in SUPPORTED_LANGUAGES:
        raise HTTPException(status_code=400, detail="unsupported preferred_language")

    u = User(
        nickname=nickname,
        email=email,
        preferred_language=preferred_language,
        password_hash=hash_password(body.password),
    )
    db.add(u)
    await db.commit()
    await db.refresh(u)
    return UserOut(id=u.id, nickname=u.nickname, email=u.email, preferred_language=u.preferred_language)


@router.post("/login", response_model=TokenOut)
async def login(body: LoginBody, db: AsyncSession = Depends(get_db)) -> TokenOut:
    nickname = body.nickname.strip()
    u = (await db.execute(select(User).where(User.nickname == nickname))).scalar_one_or_none()
    if not u or not verify_password(body.password, u.password_hash):
        raise HTTPException(status_code=401, detail="invalid credentials")
    token = create_access_token(sub=str(u.id))
    return TokenOut(access_token=token)


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(get_current_user)) -> UserOut:
    return UserOut(
        id=user.id,
        nickname=user.nickname,
        email=user.email,
        preferred_language=normalize_preferred_language(getattr(user, "preferred_language", DEFAULT_LANGUAGE)),
    )


async def _update_user_language(
    body: UpdateLanguageBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserOut:
    lang = normalize_preferred_language(body.preferred_language)
    user.preferred_language = lang
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return UserOut(id=user.id, nickname=user.nickname, email=user.email, preferred_language=user.preferred_language)


@router.patch("/me/language", response_model=UserOut)
async def update_me_language(
    body: UpdateLanguageBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserOut:
    return await _update_user_language(body=body, user=user, db=db)


# Compatibility alias for older frontend builds/proxies.
@router.patch("/language", response_model=UserOut)
async def update_language_alias(
    body: UpdateLanguageBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserOut:
    return await _update_user_language(body=body, user=user, db=db)

