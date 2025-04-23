from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional


class MarketFactSchema(BaseModel):
    market_id: str
    event_id: str
    question: str
    condition_id: str
    start_date: datetime
    end_date: datetime
    liquidity: float
    volume: float
    active: bool
    closed: bool
    submitted_by: str
    resolved_by: Optional[str] = None
    timestamp_id: int
    risk_id: Optional[int] = None
    valid_from: datetime
    valid_to: Optional[datetime] = None
    is_current: bool = True
