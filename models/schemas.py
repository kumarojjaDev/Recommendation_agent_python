from typing import List, Optional
from pydantic import BaseModel, Field

class Product(BaseModel):
    id: int
    name: str
    category: str
    brand: Optional[str] = None
    model: Optional[str] = None
    attributes: dict = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)

class PublicProduct(BaseModel):
    id: int
    name: str
    category: str
    brand: Optional[str] = None
    model: Optional[str] = None

class RecommendationRequest(BaseModel):
    item_name: str
    limit: int = 5

class RecommendationResponse(BaseModel):
    primary_item: Optional[PublicProduct]
    recommendations: List[PublicProduct]