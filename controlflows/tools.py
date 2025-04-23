import controlflow as cf
from controlflow import tool
from pydantic import BaseModel


class PriceData(BaseModel):
    open: float
    high: float
    low: float
    close: float


@tool(description="Get current day price information")
def get_daily_price_data() -> PriceData:
    """Get current day price information"""
    # Implementation details...


# price_task = cf.Task(
#     "Provide a price report for Bitcoin",
#     tools=[get_detailed_price_data]
# )