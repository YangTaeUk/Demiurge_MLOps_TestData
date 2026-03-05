"""Category E: Text 데이터셋 스키마 — StackOverflowQuestion"""

from __future__ import annotations

from pydantic import BaseModel, Field


class StackOverflowQuestion(BaseModel):
    """Stack Overflow Q&A — 질문 레코드"""

    id: int = Field(..., alias="Id")
    title: str | None = Field(None, alias="Title")
    body: str | None = Field(None, alias="Body")
    tags: str | None = Field(None, alias="Tags")
    score: int | None = Field(None, alias="Score")
    owner_user_id: int | None = Field(None, alias="OwnerUserId")
    creation_date: str | None = Field(None, alias="CreationDate")
    answer_count: int | None = Field(None, alias="AnswerCount")
    favorite_count: int | None = Field(None, alias="FavoriteCount")

    model_config = {"extra": "allow", "populate_by_name": True}


class StackOverflowAnswer(BaseModel):
    """Stack Overflow Q&A — 답변 레코드"""

    id: int = Field(..., alias="Id")
    parent_id: int | None = Field(None, alias="ParentId")
    body: str | None = Field(None, alias="Body")
    score: int | None = Field(None, alias="Score")
    owner_user_id: int | None = Field(None, alias="OwnerUserId")
    creation_date: str | None = Field(None, alias="CreationDate")
    is_accepted: bool | None = Field(None, alias="IsAcceptedAnswer")

    model_config = {"extra": "allow", "populate_by_name": True}


class EnronEmail(BaseModel):
    """Enron Email Dataset — 이메일 레코드"""

    file: str | None = None
    message: str | None = None
    from_addr: str | None = None
    to_addr: str | None = None
    subject: str | None = None
    date: str | None = None
    body: str | None = None

    model_config = {"extra": "allow"}


class GitHubRepoMetadata(BaseModel):
    """GitHub Repository Metadata — 레포지토리 메타데이터"""

    repo_id: int | None = None
    name: str | None = None
    full_name: str | None = None
    owner: str | None = None
    description: str | None = None
    language: str | None = None
    stargazers_count: int | None = Field(None, ge=0)
    forks_count: int | None = Field(None, ge=0)
    topics: str | None = None
    created_at: str | None = None

    model_config = {"extra": "allow"}
