import controlflow as cf
from agents import analyst_agent
from tools import get_daily_price_data


@cf.flow(
    default_agent=analyst_agent
)
def market_evaluation_flow(topic: str = None) -> str:
    fetch_price_data = cf.Task(
        "Get the latest market price data",
        result_type=dict,
        tools=[get_daily_price_data]
    )

    analyze_data = cf.Task(
        "Analyze gathered data and recommend the most likely outcome of the market",
        context={'market': topic},
        result_type=dict,
        depends_on=[fetch_price_data]
    )

    return analyze_data
