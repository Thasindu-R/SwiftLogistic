"""
Auth Service – route handlers for registration, login, user & profile management.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func as sqlfunc, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.database import get_db
from shared.common.security import (
    create_access_token,
    get_current_user,
    hash_password,
    require_role,
    verify_password,
)
from shared.contracts.user_schemas import (
    AdminUserCreate,
    AdminUserUpdate,
    MessageResponse,
    PasswordChange,
    ProfileUpdate,
    TokenResponse,
    UserListResponse,
    UserLogin,
    UserRegister,
    UserResponse,
)

from .models import User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/auth", tags=["Authentication"])


# ── Self-register (client only) ──────────────────────────────
@router.post("/register", response_model=TokenResponse, status_code=201)
async def register(payload: UserRegister, db: AsyncSession = Depends(get_db)):
    """Self-registration – creates a *client* account and returns a JWT."""
    existing = await db.execute(
        select(User).where(
            (User.username == payload.username) | (User.email == payload.email)
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username or email already registered",
        )

    user = User(
        username=payload.username,
        email=payload.email,
        password_hash=hash_password(payload.password),
        full_name=payload.full_name,
        phone=payload.phone,
        role="client",  # self-register always client
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)

    token = create_access_token(
        {"sub": str(user.id), "username": user.username, "role": user.role}
    )
    logger.info("User self-registered: %s (role=%s)", user.username, user.role)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


# ── Login ────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
async def login(payload: UserLogin, db: AsyncSession = Depends(get_db)):
    """Authenticate by username and return a JWT."""
    result = await db.execute(
        select(User).where(User.username == payload.username)
    )
    user = result.scalar_one_or_none()

    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated. Contact your administrator.",
        )

    token = create_access_token(
        {"sub": str(user.id), "username": user.username, "role": user.role}
    )
    logger.info("User logged in: %s (role=%s)", user.username, user.role)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


# ── Profile (protected) ─────────────────────────────────────
@router.get("/me", response_model=UserResponse)
async def get_profile(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the profile of the currently authenticated user."""
    result = await db.execute(
        select(User).where(User.id == int(current_user["sub"]))
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse.model_validate(user)


@router.put("/me", response_model=UserResponse)
async def update_profile(
    payload: ProfileUpdate,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update the authenticated user's profile fields."""
    result = await db.execute(
        select(User).where(User.id == int(current_user["sub"]))
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.full_name is not None:
        user.full_name = payload.full_name
    if payload.phone is not None:
        user.phone = payload.phone
    if payload.email is not None:
        # Check email uniqueness
        dup = await db.execute(
            select(User).where(User.email == payload.email, User.id != user.id)
        )
        if dup.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Email already in use")
        user.email = payload.email

    await db.flush()
    await db.refresh(user)
    logger.info("Profile updated: %s", user.username)
    return UserResponse.model_validate(user)


@router.put("/me/password", response_model=MessageResponse)
async def change_password(
    payload: PasswordChange,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Change the authenticated user's password."""
    result = await db.execute(
        select(User).where(User.id == int(current_user["sub"]))
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    user.password_hash = hash_password(payload.new_password)
    await db.flush()
    logger.info("Password changed for user: %s", user.username)
    return MessageResponse(message="Password updated successfully")


# ── Admin: list users ────────────────────────────────────────
@router.get("/users", response_model=UserListResponse)
async def list_users(
    role: str = Query(None, pattern="^(client|driver|admin)$"),
    search: str = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: list registered users with optional filtering."""
    query = select(User)
    count_query = select(sqlfunc.count(User.id))

    if role:
        query = query.where(User.role == role)
        count_query = count_query.where(User.role == role)
    if search:
        like = f"%{search}%"
        query = query.where(
            (User.username.ilike(like))
            | (User.full_name.ilike(like))
            | (User.email.ilike(like))
        )
        count_query = count_query.where(
            (User.username.ilike(like))
            | (User.full_name.ilike(like))
            | (User.email.ilike(like))
        )

    total = (await db.execute(count_query)).scalar() or 0
    result = await db.execute(query.order_by(User.id).offset(skip).limit(limit))
    users = result.scalars().all()

    return UserListResponse(
        users=[UserResponse.model_validate(u) for u in users],
        total=total,
    )


# ── Admin: create user ──────────────────────────────────────
@router.post("/users", response_model=UserResponse, status_code=201)
async def admin_create_user(
    payload: AdminUserCreate,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: create a user account with any role."""
    existing = await db.execute(
        select(User).where(
            (User.username == payload.username) | (User.email == payload.email)
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Username or email already exists")

    user = User(
        username=payload.username,
        email=payload.email,
        password_hash=hash_password(payload.password),
        full_name=payload.full_name,
        phone=payload.phone,
        role=payload.role,
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)
    logger.info("Admin created user: %s (role=%s)", user.username, user.role)
    return UserResponse.model_validate(user)


# ── Admin: update user ──────────────────────────────────────
@router.put("/users/{user_id}", response_model=UserResponse)
async def admin_update_user(
    user_id: int,
    payload: AdminUserUpdate,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: update any user's profile or role."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.full_name is not None:
        user.full_name = payload.full_name
    if payload.phone is not None:
        user.phone = payload.phone
    if payload.email is not None:
        dup = await db.execute(
            select(User).where(User.email == payload.email, User.id != user.id)
        )
        if dup.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Email already in use")
        user.email = payload.email
    if payload.role is not None:
        user.role = payload.role
    if payload.is_active is not None:
        user.is_active = payload.is_active

    await db.flush()
    await db.refresh(user)
    logger.info("Admin updated user %d: %s", user_id, user.username)
    return UserResponse.model_validate(user)


# ── Admin: toggle user active status ─────────────────────────
@router.patch("/users/{user_id}/status", response_model=UserResponse)
async def admin_toggle_user_status(
    user_id: int,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: toggle a user's active/inactive status."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_active = not user.is_active
    await db.flush()
    await db.refresh(user)
    status_str = "activated" if user.is_active else "deactivated"
    logger.info("Admin %s user %d: %s", status_str, user_id, user.username)
    return UserResponse.model_validate(user)


# ── Get single user (admin) ─────────────────────────────────
@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: get a single user's details."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse.model_validate(user)


# ── Token verification (used internally by api-gateway) ──────
@router.get("/verify", response_model=dict)
async def verify_token(current_user: dict = Depends(get_current_user)):
    """Verify that the bearer token is valid and return claims."""
    return current_user


# ── Health ───────────────────────────────────────────────────
@router.get("/health")
async def health():
    return MessageResponse(message="auth-service is healthy")
