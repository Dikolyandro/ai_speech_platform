from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import create_access_token, get_current_user, hash_password, verify_password
from app.db.models import User
from app.db.session import get_db
from app.services.email_service import email_service
from app.services.i18n_service import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, normalize_preferred_language


router = APIRouter(prefix="/auth", tags=["auth"])
EMAIL_VERIFICATION_CODE_TTL_MINUTES = 10
EMAIL_VERIFICATION_MAX_ATTEMPTS = 5


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


class MessageOut(BaseModel):
    message: str


class UserOut(BaseModel):
    id: int
    nickname: str
    email: EmailStr | None = None
    preferred_language: str = DEFAULT_LANGUAGE
    is_email_verified: bool = False


class RegisterOut(UserOut):
    verification_required: bool = True


class VerifyEmailBody(BaseModel):
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6, pattern=r"^\d{6}$")


class ResendVerificationCodeBody(BaseModel):
    email: EmailStr


class UpdateLanguageBody(BaseModel):
    preferred_language: str = Field(..., min_length=2, max_length=2)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_expires_at(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _new_verification_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _set_email_verification_code(user: User) -> str:
    code = _new_verification_code()
    user.email_verification_code_hash = hash_password(code)
    user.email_verification_expires_at = _utcnow() + timedelta(minutes=EMAIL_VERIFICATION_CODE_TTL_MINUTES)
    user.email_verification_attempts = 0
    return code


def _user_out(user: User, *, verification_required: bool | None = None) -> UserOut | RegisterOut:
    data = {
        "id": user.id,
        "nickname": user.nickname,
        "email": user.email,
        "preferred_language": user.preferred_language,
        "is_email_verified": bool(getattr(user, "is_email_verified", False)),
    }
    if verification_required is not None:
        return RegisterOut(**data, verification_required=verification_required)
    return UserOut(**data)


@router.post("/register", response_model=RegisterOut)
async def register(
    body: RegisterBody,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> RegisterOut:
    nickname = body.nickname.strip()
    if not nickname or " " in nickname:
        raise HTTPException(status_code=400, detail="nickname must not contain spaces")
    nick_lower = nickname.lower()
    exists_n = (
        await db.execute(select(User).where(func.lower(User.nickname) == nick_lower))
    ).scalar_one_or_none()
    if exists_n:
        raise HTTPException(status_code=409, detail="nickname already taken")

    email = body.email.strip().lower()
    exists_e = (await db.execute(select(User).where(func.lower(User.email) == email))).scalar_one_or_none()
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
        is_email_verified=False,
    )
    code = _set_email_verification_code(u)
    db.add(u)
    try:
        await db.commit()
        await db.refresh(u)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="nickname or email already taken") from None
    background_tasks.add_task(email_service.send_verification_code, to_email=email, code=code)
    return _user_out(u, verification_required=True)


@router.post("/verify-email", response_model=MessageOut)
async def verify_email(body: VerifyEmailBody, db: AsyncSession = Depends(get_db)) -> MessageOut:
    email = body.email.strip().lower()
    user = (await db.execute(select(User).where(func.lower(User.email) == email))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid verification code")
    if user.is_email_verified:
        return MessageOut(message="Email already verified")
    if user.email_verification_attempts >= EMAIL_VERIFICATION_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new code.")

    expires_at = _normalize_expires_at(user.email_verification_expires_at)
    if not user.email_verification_code_hash or not expires_at or expires_at < _utcnow():
        raise HTTPException(status_code=400, detail="Verification code has expired. Please request a new code.")

    if not verify_password(body.code, user.email_verification_code_hash):
        user.email_verification_attempts += 1
        db.add(user)
        await db.commit()
        raise HTTPException(status_code=400, detail="Invalid verification code")

    user.is_email_verified = True
    user.email_verification_code_hash = None
    user.email_verification_expires_at = None
    user.email_verification_attempts = 0
    db.add(user)
    await db.commit()
    return MessageOut(message="Email verified successfully")


@router.post("/resend-verification-code", response_model=MessageOut)
async def resend_verification_code(
    body: ResendVerificationCodeBody,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> MessageOut:
    email = body.email.strip().lower()
    user = (await db.execute(select(User).where(func.lower(User.email) == email))).scalar_one_or_none()
    if user and not user.is_email_verified:
        code = _set_email_verification_code(user)
        db.add(user)
        await db.commit()
        background_tasks.add_task(email_service.send_verification_code, to_email=email, code=code)
    return MessageOut(message="If this email needs verification, a new code has been sent.")


@router.post("/login", response_model=TokenOut)
async def login(body: LoginBody, db: AsyncSession = Depends(get_db)) -> TokenOut:
    nickname = body.nickname.strip()
    u = (
        await db.execute(select(User).where(func.lower(User.nickname) == nickname.lower()))
    ).scalar_one_or_none()
    if not u or not verify_password(body.password, u.password_hash):
        raise HTTPException(status_code=401, detail="invalid credentials")
    if not u.is_email_verified:
        raise HTTPException(status_code=403, detail="Please verify your email before logging in.")
    token = create_access_token(sub=str(u.id))
    return TokenOut(access_token=token)


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(get_current_user)) -> UserOut:
    return UserOut(
        id=user.id,
        nickname=user.nickname,
        email=user.email,
        preferred_language=normalize_preferred_language(getattr(user, "preferred_language", DEFAULT_LANGUAGE)),
        is_email_verified=bool(getattr(user, "is_email_verified", False)),
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
    return UserOut(
        id=user.id,
        nickname=user.nickname,
        email=user.email,
        preferred_language=user.preferred_language,
        is_email_verified=bool(user.is_email_verified),
    )


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
