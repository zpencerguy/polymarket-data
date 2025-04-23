from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional


class EventFactSchema(BaseModel):
    event_id: str
    ticker: str
    slug: str
    title: str
    description: Optional[str] = None
    start_date: datetime
    creation_date: datetime
    end_date: datetime
    image_url: Optional[str] = None
    icon_url: Optional[str] = None
    active: bool
    closed: bool
    archived: bool
    volume: float
    liquidity: float
    open_interest: float
    timestamp_id: int
    valid_from: datetime
    valid_to: Optional[datetime] = None
    is_current: bool = True

