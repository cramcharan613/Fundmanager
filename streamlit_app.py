import asyncio
from typing import Dict, Optional, List, Any
from datetime import datetime
import logging
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, DataReturnMode, GridUpdateMode
import pandas as pd
import json
import io
import xlsxwriter
import time
import aiohttp
from functools import lru_cache
import concurrent.futures
from dataclasses import dataclass
from streamlit_javascript import st_javascript

# Enhanced page config with custom theme
st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced visuals
st.markdown("""
<style>
    /* Main app styling */
    .stApp {
        max-width: 100%;
        padding: 1rem;
    }
    
    /* Card-like containers */
    .stats-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.1), rgba(255,255,255,0.05));
        border-radius: 10px;
        padding: 1.5rem;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 1rem;
        transition: transform 0.3s ease;
    }
    
    .stats-card:hover {
        transform: translateY(-5px);
    }
    
    /* Enhanced headers */
    h1 {
        background: linear-gradient(45deg, #2196F3, #21CBF3);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
    }
    
    /* Custom button styling */
    .stButton>button {
        background: linear-gradient(45deg, #2196F3, #21CBF3);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 5px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Enhanced select boxes */
    .stSelectbox {
        border-radius: 5px;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(255,255,255,0.1);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: rgba(33,150,243,0.5);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(33,150,243,0.8);
    }
    
    /* AG-Grid custom styling */
    .ag-theme-streamlit .ag-root-wrapper {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Loading animation */
    .loading-spinner {
        display: inline-block;
        width: 50px;
        height: 50px;
        border: 3px solid rgba(255,255,255,.3);
        border-radius: 50%;
        border-top-color: #2196F3;
        animation: spin 1s ease-in-out infinite;
    }
    
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
</style>
""", unsafe_allow_html=True)

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ETFData:
    """Data class for ETF information"""
    ticker: str
    issuer: str
    description: str
    asset_class: str
    inception_date: str
    aum: float
    expense_ratio: float
    holdings: int

class CachedETFDataFetcher:
    """Enhanced ETF data fetcher with caching and parallel processing"""
    def __init__(self):
        self.stockanalysis_url = (
            "https://api.stockanalysis.com/api/screener/e/bd/"
            "issuer+n+assetClass+inceptionDate+exchange+etfLeverage+"
            "aum+close+holdings+price+cusip+isin+etfCategory+"
            "expenseRatio+etfIndex+etfRegion+etfCountry+optionable.json"
        )
        self.cache_timeout = 3600  # 1 hour cache

    @lru_cache(maxsize=1)
    async def fetch_data(self) -> pd.DataFrame:
        """Fetch ETF data with caching"""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.stockanalysis_url) as resp:
                raw_data = await resp.json()
                return self._process_data(raw_data)

    def _process_data(self, raw_data: Dict) -> pd.DataFrame:
        """Process raw data with parallel processing"""
        if not raw_data or 'data' not in raw_data:
            return pd.DataFrame()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            processed_data = list(executor.map(
                self._process_etf_entry,
                raw_data['data']['data'].items()
            ))

        df = pd.DataFrame(processed_data)
        return self._enhance_dataframe(df)

    @staticmethod
    def _process_etf_entry(entry: tuple) -> Dict[str, Any]:
        """Process individual ETF entry"""
        ticker, data = entry
        return {
            'TICKER_SYMBOL': ticker,
            'ETF_ISSUER': data.get('issuer', ''),
            'ETF_DESCRIPTION': data.get('n', ''),
            'ASSET_CLASS': data.get('assetClass', ''),
            'INCEPTION_DATE': data.get('inceptionDate', ''),
            'ASSETS_UNDER_MANAGEMENT': data.get('aum', 0),
            'EXPENSE_RATIO': data.get('expenseRatio', 0),
            'NUMBER_OF_HOLDINGS': data.get('holdings', 0),
            'CURRENT_PRICE': data.get('price', 0),
            'CUSIP': data.get('cusip', ''),
            'ETF_CATEGORY': data.get('etfCategory', ''),
            'TRACKING_INDEX': data.get('etfIndex', ''),
            'GEOGRAPHIC_REGION': data.get('etfRegion', ''),
            'COUNTRY_FOCUS': data.get('etfCountry', ''),
            'HAS_OPTIONS': data.get('optionable', False)
        }

    def _enhance_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhance dataframe with additional metrics and formatting"""
        # Format numeric columns
        numeric_cols = {
            'CURRENT_PRICE': '${:,.2f}',
            'ASSETS_UNDER_MANAGEMENT': '${:,.2f}M',
            'EXPENSE_RATIO': '{:.2%}'
        }

        for col, fmt in numeric_cols.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: fmt.format(x) if pd.notnull(x) else '')

        # Add calculated metrics
        df['YTD_PERFORMANCE'] = df['CURRENT_PRICE'].apply(
            lambda x: f"{(float(x.replace('$', '').replace(',', '')) / 100 - 1):.2%}"
            if isinstance(x, str) and x
            else ''
        )

        return df

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Load ETF data with caching"""
    fetcher = CachedETFDataFetcher()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetcher.fetch_data())

def create_interactive_chart(ticker: str) -> None:
    """Create interactive TradingView chart"""
    chart_config = {
        "symbol": ticker,
        "interval": "D",
        "timezone": "Etc/UTC",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "enable_publishing": False,
        "allow_symbol_change": True,
        "save_image": True,
        "studies": [
            "MASimple@tv-basicstudies",
            "RSI@tv-basicstudies",
            "MACD@tv-basicstudies",
            "BB@tv-basicstudies"
        ]
    }
    
    st.components.v1.html(
        f"""
        <div class="tradingview-widget-container">
            <div id="tradingview_chart"></div>
            <script src="https://s3.tradingview.com/tv.js"></script>
            <script>
                new TradingView.widget({chart_config});
            </script>
        </div>
        """,
        height=600
    )

def main() -> None:
    st.title("📈 ETF Explorer Pro")
    
    # Add app description with enhanced markdown
    st.markdown("""
    <div class="stats-card">
        <h3>Welcome to ETF Explorer Pro</h3>
        <p>An advanced ETF analysis platform with real-time data, interactive charts, and comprehensive filtering.</p>
    </div>
    """, unsafe_allow_html=True)

    # Load data with progress indicator
    with st.spinner("Loading ETF data..."):
        etf_data = load_data()
        if etf_data.empty:
            st.error("❌ No data available. Please try again later.")
            return

    # Create layout
    col1, col2, col3 = st.columns([2, 6, 2])

    with col1:
        st.markdown('<div class="stats-card">', unsafe_allow_html=True)
        
        # Enhanced filters
        selected_issuer = st.selectbox(
            "🏢 ETF Issuer",
            options=["All"] + sorted(etf_data['ETF_ISSUER'].unique().tolist())
        )
        
        selected_asset_class = st.selectbox(
            "💼 Asset Class",
            options=["All"] + sorted(etf_data['ASSET_CLASS'].unique().tolist())
        )
        
        min_aum = st.slider(
            "💰 Min AUM ($M)",
            min_value=0,
            max_value=int(etf_data['ASSETS_UNDER_MANAGEMENT'].max()),
            value=0
        )
        
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        # Filter data based on selections
        filtered_data = etf_data.copy()
        if selected_issuer != "All":
            filtered_data = filtered_data[filtered_data['ETF_ISSUER'] == selected_issuer]
        if selected_asset_class != "All":
            filtered_data = filtered_data[filtered_data['ASSET_CLASS'] == selected_asset_class]
        filtered_data = filtered_data[
            filtered_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '').str.replace(',', '').astype(float) >= min_aum
        ]

        # Display interactive grid
        gb = GridOptionsBuilder.from_dataframe(filtered_data)
        gb.configure_default_column(
            editable=False,
            sortable=True,
            filterable=True,
            resizable=True
        )
        
        # Add chart button
        gb.configure_column(
            "TICKER_SYMBOL",
            cellRenderer=JsCode("""
            function(params) {
                return `
                    <div style="display: flex; align-items: center;">
                        <span style="margin-right: 8px;">${params.value}</span>
                        <button onclick="showChart('${params.value}')" 
                                style="background: linear-gradient(45deg, #2196F3, #21CBF3);
                                       color: white;
                                       border: none;
                                       padding: 4px 8px;
                                       border-radius: 4px;
                                       cursor: pointer;">
                            📈 Chart
                        </button>
                    </div>
                `;
            }
            """)
        )

        grid_response = AgGrid(
            filtered_data,
            gridOptions=gb.build(),
            height=600,
            enable_enterprise_modules=True,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            theme='streamlit'
        )

    with col3:
        st.markdown('<div class="stats-card">', unsafe_allow_html=True)
        st.subheader("📊 Summary Statistics")
        
        total_etfs = len(filtered_data)
        total_aum = filtered_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '').str.replace(',', '').astype(float).sum()
        avg_expense = filtered_data['EXPENSE_RATIO'].str.rstrip('%').astype(float).mean()
        
        st.metric("Total ETFs", f"{total_etfs:,}")
        st.metric("Total AUM", f"${total_aum:,.2f}M")
        st.metric("Avg Expense Ratio", f"{avg_expense:.2%}")
        
        # Export options
        st.download_button(
            label="📥 Export to CSV",
            data=filtered_data.to_csv(index=False).encode('utf-8'),
            file_name=f"etf_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
