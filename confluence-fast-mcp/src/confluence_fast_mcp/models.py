"""Pydantic models for MCP responses."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class SpaceResponse(BaseModel):
    """Confluence space response model."""
    id: str
    key: str
    name: str
    type: str = "global"
    status: str = "current"

    class Config:
        extra = "allow"


class PageVersion(BaseModel):
    """Page version information."""
    number: int
    when: str
    by: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class PageBody(BaseModel):
    """Page body content."""
    storage: Optional[Dict[str, str]] = None
    atlas_doc_format: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class PageResponse(BaseModel):
    """Confluence page response model."""
    id: str
    type: str = "page"
    status: str = "current"
    title: str
    space: Optional[Dict[str, str]] = None
    version: Optional[PageVersion] = None
    body: Optional[PageBody] = None
    history: Optional[Dict[str, Any]] = None
    _links: Optional[Dict[str, str]] = None

    class Config:
        extra = "allow"


class SearchResult(BaseModel):
    """Search result model."""
    results: List[PageResponse]
    start: int = 0
    limit: int = 25
    size: int
    _links: Optional[Dict[str, str]] = None

    class Config:
        extra = "allow"


class ResourceResponse(BaseModel):
    """Atlassian resource response."""
    id: str
    name: str
    url: str
    scopes: List[str] = []
    avatarUrl: Optional[str] = None

    class Config:
        extra = "allow"


class UserInfoResponse(BaseModel):
    """User information response."""
    accountId: str = "local-user"
    accountType: str = "atlassian"
    email: str = "local@example.com"
    displayName: str = "Local User"

    class Config:
        extra = "allow"
