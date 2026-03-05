"""Category B: Document 데이터셋 스키마 — InstacartOrder, TmdbMovie"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InstacartProduct(BaseModel):
    """Instacart 상품"""

    product_id: int
    product_name: str | None = None
    aisle: str | None = None
    department: str | None = None
    add_to_cart_order: int | None = None
    reordered: int | None = None

    model_config = {"extra": "allow"}


class InstacartOrder(BaseModel):
    """Instacart Market Basket — 주문 + 중첩 상품 문서"""

    order_id: int
    user_id: int | None = None
    order_number: int | None = None
    order_dow: int | None = Field(None, ge=0, le=6)
    order_hour_of_day: int | None = Field(None, ge=0, le=23)
    days_since_prior_order: float | None = None
    products: list[InstacartProduct] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class TmdbMovie(BaseModel):
    """TMDB Movie Metadata — 영화 메타 + JSON 파싱 필드"""

    id: int
    title: str | None = None
    original_title: str | None = None
    overview: str | None = None
    release_date: str | None = None
    budget: int | None = Field(None, ge=0)
    revenue: int | None = Field(None, ge=0)
    runtime: float | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    popularity: float | None = None
    genres: list[dict[str, Any]] = Field(default_factory=list)
    cast: list[dict[str, Any]] = Field(default_factory=list)
    crew: list[dict[str, Any]] = Field(default_factory=list)
    keywords: list[dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class AirbnbListing(BaseModel):
    """Airbnb Seattle — 숙소 리스팅 + 리뷰 + 지리 좌표"""

    id: int
    name: str | None = None
    host_id: int | None = None
    host_name: str | None = None
    neighbourhood: str | None = None
    latitude: float | None = Field(None, ge=-90, le=90)
    longitude: float | None = Field(None, ge=-180, le=180)
    room_type: str | None = None
    price: float | None = None
    number_of_reviews: int | None = None
    availability_365: int | None = None

    model_config = {"extra": "allow"}


class AmazonReview(BaseModel):
    """Amazon Reviews — 대량 리뷰 텍스트 + 감성 라벨"""

    reviewerID: str | None = None
    asin: str | None = None
    reviewerName: str | None = None
    overall: float | None = Field(None, ge=1, le=5)
    reviewText: str | None = None
    summary: str | None = None
    unixReviewTime: int | None = None
    reviewTime: str | None = None

    model_config = {"extra": "allow"}


class YelpBusiness(BaseModel):
    """Yelp Dataset — 비즈니스 레코드"""

    business_id: str
    name: str | None = None
    city: str | None = None
    state: str | None = None
    latitude: float | None = Field(None, ge=-90, le=90)
    longitude: float | None = Field(None, ge=-180, le=180)
    stars: float | None = Field(None, ge=1, le=5)
    review_count: int | None = Field(None, ge=0)
    categories: str | None = None

    model_config = {"extra": "allow"}


class FoodComRecipe(BaseModel):
    """Food.com Recipes — 레시피 레코드"""

    recipe_id: int | None = Field(None, alias="id")
    name: str | None = None
    minutes: int | None = None
    n_steps: int | None = None
    n_ingredients: int | None = None
    description: str | None = None
    ingredients: list | None = None
    steps: list | None = None

    model_config = {"extra": "allow", "populate_by_name": True}
