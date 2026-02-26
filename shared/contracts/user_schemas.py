"""Pydantic schemas for user registration, login, and profile."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


# ── Auth schemas ─────────────────────────────────────────────
class UserLogin(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)


class UserRegister(BaseModel):
    """Self-registration (creates client accounts only)."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    full_name: str = Field(..., min_length=1, max_length=100)
    phone: str = Field(default="", max_length=20)
    password: str = Field(..., min_length=6)


# ── Response schemas ─────────────────────────────────────────
class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: str
    phone: str
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class MessageResponse(BaseModel):
    message: str
    detail: Optional[str] = None


# ── Profile management ───────────────────────────────────────
class ProfileUpdate(BaseModel):
    full_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    email: Optional[EmailStr] = None


class PasswordChange(BaseModel):
    current_password: str = Field(..., min_length=6)
    new_password: str = Field(..., min_length=6)


# ── Admin user management ────────────────────────────────────
class AdminUserCreate(BaseModel):
    """Admin can create users with any role."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    full_name: str = Field(..., min_length=1, max_length=100)
    phone: str = Field(default="", max_length=20)
    password: str = Field(..., min_length=6)
    role: str = Field(default="client", pattern="^(client|driver|admin)$")


class AdminUserUpdate(BaseModel):
    full_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    email: Optional[EmailStr] = None
    role: Optional[str] = Field(None, pattern="^(client|driver|admin)$")
    is_active: Optional[bool] = None


class UserListResponse(BaseModel):
    users: list[UserResponse]
    total: int
