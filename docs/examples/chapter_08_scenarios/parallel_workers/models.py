# models.py - Shared Model Definitions for Parallel Worker Experiments (PostgreSQL Version)
# docs/examples/chapter_08_scenarios/parallel_workers/models.py
"""
Model hierarchy:
    User  --(has_many)--> Post  --(has_many)--> Comment
    User  <--(belongs_to)-- Post
    Post  <--(belongs_to)-- Comment
    User  <--(belongs_to)-- Comment (comment author)

Each model provides both synchronous (inheriting ActiveRecord) and asynchronous
(inheriting AsyncActiveRecord) versions with identical method names.
Async version only requires adding await.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Optional

from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.field import IntegerPKMixin, TimestampMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.relation import BelongsTo, HasMany
from rhosocial.activerecord.relation.async_descriptors import (
    AsyncBelongsTo,
    AsyncHasMany,
)


# ─────────────────────────────────────────
# Synchronous Models
# ─────────────────────────────────────────


class User(IntegerPKMixin, TimestampMixin, ActiveRecord):
    """Blog user (synchronous)"""

    __table_name__ = "users"

    id: Optional[int] = None
    username: str
    email: str
    is_active: bool = True

    c: ClassVar[FieldProxy] = FieldProxy()

    posts: ClassVar[HasMany[Post]] = HasMany(foreign_key="user_id", inverse_of="author")
    comments: ClassVar[HasMany[Comment]] = HasMany(foreign_key="user_id", inverse_of="author")


class Post(IntegerPKMixin, TimestampMixin, ActiveRecord):
    """Blog post (synchronous)"""

    __table_name__ = "posts"

    id: Optional[int] = None
    user_id: int
    title: str
    body: str
    status: str = "draft"  # draft / processing / published
    view_count: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()

    author: ClassVar[BelongsTo[User]] = BelongsTo(foreign_key="user_id", inverse_of="posts")
    comments: ClassVar[HasMany[Comment]] = HasMany(foreign_key="post_id", inverse_of="post")


class Comment(IntegerPKMixin, TimestampMixin, ActiveRecord):
    """Comment (synchronous)"""

    __table_name__ = "comments"

    id: Optional[int] = None
    post_id: int
    user_id: int
    body: str
    is_approved: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()

    post: ClassVar[BelongsTo[Post]] = BelongsTo(foreign_key="post_id", inverse_of="comments")
    author: ClassVar[BelongsTo[User]] = BelongsTo(foreign_key="user_id", inverse_of="comments")


# ─────────────────────────────────────────
# Asynchronous Models (identical method names to sync version, add await)
# ─────────────────────────────────────────


class AsyncUser(IntegerPKMixin, TimestampMixin, AsyncActiveRecord):
    """Blog user (asynchronous)"""

    __table_name__ = "users"

    id: Optional[int] = None
    username: str
    email: str
    is_active: bool = True

    c: ClassVar[FieldProxy] = FieldProxy()

    posts: ClassVar[AsyncHasMany[AsyncPost]] = AsyncHasMany(foreign_key="user_id", inverse_of="author")
    comments: ClassVar[AsyncHasMany[AsyncComment]] = AsyncHasMany(foreign_key="user_id", inverse_of="author")


class AsyncPost(IntegerPKMixin, TimestampMixin, AsyncActiveRecord):
    """Blog post (asynchronous)"""

    __table_name__ = "posts"

    id: Optional[int] = None
    user_id: int
    title: str
    body: str
    status: str = "draft"
    view_count: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()

    author: ClassVar[AsyncBelongsTo[AsyncUser]] = AsyncBelongsTo(foreign_key="user_id", inverse_of="posts")
    comments: ClassVar[AsyncHasMany[AsyncComment]] = AsyncHasMany(foreign_key="post_id", inverse_of="post")


class AsyncComment(IntegerPKMixin, TimestampMixin, AsyncActiveRecord):
    """Comment (asynchronous)"""

    __table_name__ = "comments"

    id: Optional[int] = None
    post_id: int
    user_id: int
    body: str
    is_approved: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()

    post: ClassVar[AsyncBelongsTo[AsyncPost]] = AsyncBelongsTo(foreign_key="post_id", inverse_of="comments")
    author: ClassVar[AsyncBelongsTo[AsyncUser]] = AsyncBelongsTo(foreign_key="user_id", inverse_of="comments")


# ─────────────────────────────────────────
# Table Creation DDL (PostgreSQL)
# ─────────────────────────────────────────

SCHEMA_SQL = """\
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    id         SERIAL       NOT NULL PRIMARY KEY,
    username   VARCHAR(64)  NOT NULL,
    email      VARCHAR(255) NOT NULL,
    is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NULL,
    CONSTRAINT uq_username UNIQUE (username)
);

CREATE TABLE posts (
    id         SERIAL       NOT NULL PRIMARY KEY,
    user_id    INTEGER      NOT NULL REFERENCES users(id),
    title      VARCHAR(255) NOT NULL,
    body       TEXT         NOT NULL DEFAULT '',
    status     VARCHAR(20)  NOT NULL DEFAULT 'draft',
    view_count INTEGER      NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITHOUT TIME ZONE NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NULL
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_status ON posts(status);

CREATE TABLE comments (
    id          SERIAL  NOT NULL PRIMARY KEY,
    post_id     INTEGER NOT NULL REFERENCES posts(id),
    user_id     INTEGER NOT NULL REFERENCES users(id),
    body        TEXT    NOT NULL,
    is_approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP WITHOUT TIME ZONE NULL,
    updated_at  TIMESTAMP WITHOUT TIME ZONE NULL
);

CREATE INDEX idx_comments_post_id ON comments(post_id);
CREATE INDEX idx_comments_user_id ON comments(user_id);
"""


def now_utc() -> datetime:
    """Return current UTC time (naive datetime, PostgreSQL friendly)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)
