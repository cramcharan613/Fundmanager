import asyncio
from typing import Dict, Optional, Any
from datetime import datetime
import logging
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode, JsCode
import pandas as pd
import io
import xlsxwriter
import aiohttp
from functools import lru_cache
import concurrent.futures
from dataclasses import dataclass
from streamlit_javascript import st_javascript

st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈"
)

# Custom CSS for a modern look
st.markdown("""
<style>
body {
    font-family: "Inter", sans-serif;
    margin: 0;
    padding: 0;
    background: var(--bg-color);
    color: var(--text-color);
}

.stApp {
    padding: 1rem;
}

h1, h2, h3 {
    font-weight: 700;
}

.highlighted-row {
    background-color: rgba(255, 215, 0, 0.3) !important;
}

.ag-root-wrapper {
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
}

.stButton>button {
    background: linear-gradient(45deg, #2196F3, #21CBF3);
    color: white !important;
    border: none;
    border-radius: 5px;
    font-weight: bold;
    transition: all 0.3s ease;
    padding: 0.5rem 1rem;
}
.stButton>button:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0,0,0,0.2);
}
</style>
""", unsafe_allow_html=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ETFData:
    ticker: str
    issuer: str
    description: str
    asset_class: str
    inception_date: str
    aum: float
    expense_ratio: float
    holdings: int

class CachedETFDataFetcher:
    def __init__(self):
        self.stockanalysis_url = (
            "https://api.stockanalysis.com/api/screener/e/bd/"
            "issuer+n+assetClass+inceptionDate+exchange+etfLeverage+"
            "aum+close+holdings+price+cusip+isin+etfCategory+"
            "expenseRatio+etfIndex+etfRegion+etfCountry+optionable.json"
        )

    @lru_cache(maxsize=1)
    async def fetch_data(self) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.stockanalysis_url) as resp:
                raw_data = await resp.json()
                return self._process_data(raw_data)

    def _process_data(self, raw_data: Dict) -> pd.DataFrame:
        if not raw_data or 'data' not in raw_data:
            return pd.DataFrame()

        raw_entries = raw_data['data']['data']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            processed_data = list(executor.map(self._process_etf_entry, raw_entries.items()))

        df = pd.DataFrame(processed_data)
        return self._enhance_dataframe(df)

    @staticmethod
    def _process_etf_entry(entry: tuple) -> Dict[str, Any]:
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
        df['CURRENT_PRICE'] = pd.to_numeric(df['CURRENT_PRICE'], errors='coerce')
        df['CURRENT_PRICE'] = df['CURRENT_PRICE'].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else '')

        df['ASSETS_UNDER_MANAGEMENT'] = pd.to_numeric(df['ASSETS_UNDER_MANAGEMENT'], errors='coerce')
        df['ASSETS_UNDER_MANAGEMENT'] = df['ASSETS_UNDER_MANAGEMENT'].apply(lambda x: f"${x:,.2f}M" if pd.notnull(x) else '')

        df['EXPENSE_RATIO'] = pd.to_numeric(df['EXPENSE_RATIO'], errors='coerce')
        df['EXPENSE_RATIO'] = df['EXPENSE_RATIO'].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else '')

        if 'CUSIP' in df.columns:
            cols = ['CUSIP'] + [c for c in df.columns if c != 'CUSIP']
            df = df[cols]

        return df

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    fetcher = CachedETFDataFetcher()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetcher.fetch_data())

class ETF:
    def __init__(self, data: Dict[str, str]):
        self.data = dict(data)  # ensure it's a dict

    def _repr_html_(self):
        html = "<table border='1' style='border-collapse: collapse; font-family: sans-serif; font-size:14px;'>"
        html += "<tr><th colspan='2' style='background:#ddd; padding:8px; text-align:center;'>ETF Details</th></tr>"
        for k, v in self.data.items():
            html += f"<tr><td style='padding:8px; background:#f7f7f7; font-weight:bold;'>{k}</td><td style='padding:8px;'>{v}</td></tr>"
        html += "</table>"
        return html

def get_grid_options() -> Dict:
    return {
        'enableRangeSelection': True,
        'enableCharts': True,
        'suppressRowClickSelection': False,
        'enableSorting': True,
        'enableFilter': True,
        'enableColResize': True,
        'rowSelection': 'multiple',
        'enableStatusBar': True,
        'enableFillHandle': True,
        'enableRangeHandle': True,
        'enableCellChangeFlash': True,
        'enableCellTextSelection': True,
        'enableClipboard': True,
        'enableGroupEdit': True,
        'enableCellExpressions': True,
        'enableBrowserTooltips': True,
        'enableAdvancedFilter': True,
        'enableContextMenu': True,
        'enableUndoRedo': True,
        'enableCsvExport': True,
        'enableExcelExport': True,
        'enablePivotMode': True,
        'enableValue': True,
        'enablePivoting': True,
        'enableRowGroup': True,
        'enableQuickFilter': True,
        'floatingFilter': True,
        'includeRowGroupColumns': True,
        'includeValueColumns': True,
        'includePivotColumns': True,
        'pagination': False,  # infinite scroll
        'rowModelType': 'clientSide'
    }

def create_enhanced_tradingview_chart(ticker: str, container_id: str) -> str:
    """
    Create an enhanced TradingView chart with advanced features
    """
    return f"""
    <div class="tradingview-widget-container" style="height: 100%; width: 100%;">
        <div id="{container_id}" style="height: calc(100vh - 200px); width: 100%;"></div>
        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
        <script type="text/javascript">
        new TradingView.widget({{
            "autosize": true,
            "symbol": "{ticker}",
            "interval": "D",
            "timezone": "Etc/UTC",
            "theme": "dark",
            "style": "1",
            "locale": "en",
            "toolbar_bg": "#f1f3f6",
            "enable_publishing": false,
            "hide_top_toolbar": false,
            "hide_legend": false,
            "save_image": true,
            "container_id": "{container_id}",
            "withdateranges": true,
            "allow_symbol_change": true,
            "watchlist": [
                "SPY",
                "QQQ",
                "IWM",
                "DIA"
            ],
            "details": true,
            "hotlist": true,
            "calendar": true,
            "studies": [
                "MASimple@tv-basicstudies",
                "RSI@tv-basicstudies",
                "MACD@tv-basicstudies",
                "BB@tv-basicstudies",
                "Volume@tv-basicstudies",
                "AwesomeOscillator@tv-basicstudies",
                "StochasticRSI@tv-basicstudies"
            ],
            "show_popup_button": true,
            "popup_width": "1000",
            "popup_height": "650",
            "drawings_access": {{
                "type": "all",
                "tools": [
                    {{
                        "name": "Regression Trend",
                        "grayed": false
                    }}
                ]
            }},
            "disabled_features": [
                "header_symbol_search",
                "header_screenshot"
            ],
            "enabled_features": [
                "study_templates",
                "use_localstorage_for_settings",
                "volume_force_overlay",
                "create_volume_indicator_by_default",
                "display_market_status",
                "header_chart_type",
                "header_compare",
                "header_indicators",
                "header_settings",
                "hide_last_na_study_output",
                "legend_context_menu",
                "show_chart_property_page",
                "support_multicharts",
                "timeframes_toolbar",
                "right_bar_stays_on_scroll",
                "chart_crosshair_menu"
            ],
            "overrides": {{
                "mainSeriesProperties.candleStyle.upColor": "#26a69a",
                "mainSeriesProperties.candleStyle.downColor": "#ef5350",
                "mainSeriesProperties.candleStyle.drawWick": true,
                "mainSeriesProperties.candleStyle.drawBorder": true,
                "mainSeriesProperties.candleStyle.borderUpColor": "#26a69a",
                "mainSeriesProperties.candleStyle.borderDownColor": "#ef5350",
                "mainSeriesProperties.candleStyle.wickUpColor": "#26a69a",
                "mainSeriesProperties.candleStyle.wickDownColor": "#ef5350",
                "paneProperties.background": "#131722",
                "paneProperties.vertGridProperties.color": "#363c4e",
                "paneProperties.horzGridProperties.color": "#363c4e",
                "scalesProperties.textColor": "#fff",
                "mainSeriesProperties.priceLineColor": "#2196f3"
            }},
            "loading_screen": {{ 
                "backgroundColor": "#131722",
                "foregroundColor": "#2196f3"
            }}
        }});
        </script>
    </div>
    """

def create_tradingview_technical_analysis(ticker: str) -> str:
    """
    Create TradingView Technical Analysis Widget
    """
    return f"""
    <div class="tradingview-widget-container">
        <div class="tradingview-widget-container__widget"></div>
        <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-technical-analysis.js">
        {{
            "interval": "1m",
            "width": "100%",
            "isTransparent": false,
            "height": "450",
            "symbol": "{ticker}",
            "showIntervalTabs": true,
            "locale": "en",
            "colorTheme": "dark"
        }}
        </script>
    </div>
    """

def create_tradingview_company_profile(ticker: str) -> str:
    """
    Create TradingView Company Profile Widget
    """
    return f"""
    <div class="tradingview-widget-container">
        <div class="tradingview-widget-container__widget"></div>
        <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-symbol-profile.js">
        {{
            "width": "100%",
            "height": "400",
            "colorTheme": "dark",
            "isTransparent": false,
            "symbol": "{ticker}",
            "locale": "en"
        }}
        </script>
    </div>
    """

def show_tradingview_analysis(ticker: str):
    """
    Display comprehensive TradingView analysis
    """
    tab1, tab2, tab3 = st.tabs(["📈 Chart", "📊 Technical Analysis", "🏢 Profile"])
    
    with tab1:
        st.components.v1.html(
            create_enhanced_tradingview_chart(ticker, "tradingview_chart"),
            height=800
        )
        
    with tab2:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.components.v1.html(
                create_tradingview_technical_analysis(ticker),
                height=500
            )
        with col2:
            st.markdown("""
            <div style="background: rgba(255,255,255,0.1); padding: 1rem; border-radius: 8px;">
                <h4>Trading Signals</h4>
                <p>Coming soon...</p>
            </div>
            """, unsafe_allow_html=True)
            
    with tab3:
        st.components.v1.html(
            create_tradingview_company_profile(ticker),
            height=450
        )

def main() -> None:
    st.title("📈 ETF Explorer Pro")

    st.markdown("""
    Explore ETFs with interactive filtering, grouping, pivoting, infinite scrolling,
    custom CSS styling, and JavaScript interactions.
    """)

    with st.spinner("Loading ETF data..."):
        etf_data = load_data()

    if etf_data.empty:
        st.error("No data available. Please try again later.")
        return

    col1, col2 = st.columns([1,9])

    with col1:
        issuers = sorted(etf_data['ETF_ISSUER'].dropna().unique().tolist())
        selected_issuer = st.selectbox("Filter by ETF Issuer", ["All"] + issuers, index=0)

        asset_classes = sorted(etf_data['ASSET_CLASS'].dropna().unique().tolist())
        selected_asset_class = st.selectbox("Filter by Asset Class", ["All"] + asset_classes, index=0)

        numeric_aum = etf_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('M','')
        numeric_aum = pd.to_numeric(numeric_aum, errors='coerce')
        max_aum = int(numeric_aum.max()) if numeric_aum.notnull().any() else 0
        min_aum = st.slider("Min AUM ($M)", min_value=0, max_value=max_aum, value=0)

        export_format = st.selectbox("Export Format", ["CSV", "Excel"], key="export_format")
        if export_format:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if export_format.lower() == 'csv':
                buffer = io.BytesIO()
                etf_data.to_csv(buffer, index=False, encoding='utf-8')
                buffer.seek(0)
                bytes_data = buffer.getvalue()
                filename = f"etf_data_{timestamp}.csv"
                mime_type = "text/csv"
            else:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    etf_data.to_excel(writer, index=False, sheet_name='ETF_Data')
                buffer.seek(0)
                bytes_data = buffer.getvalue()
                filename = f"etf_data_{timestamp}.xlsx"
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            
            st.download_button(
                label=f"Download {export_format}",
                data=bytes_data,
                file_name=filename,
                mime=mime_type
            )

    # Filter data
    filtered_data = etf_data.copy()
    if selected_issuer != "All":
        filtered_data = filtered_data[filtered_data['ETF_ISSUER'] == selected_issuer]
    if selected_asset_class != "All":
        filtered_data = filtered_data[filtered_data['ASSET_CLASS'] == selected_asset_class]

    numeric_filtered_aum = filtered_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$','',regex=False).str.replace(',','',regex=False).str.replace('M','')
    numeric_filtered_aum = pd.to_numeric(numeric_filtered_aum, errors='coerce')
    filtered_data = filtered_data[numeric_filtered_aum >= min_aum]

    with col2:
        quick_search = st.text_input("Global Quick Search", value="", help="Type to filter all columns globally")

        gb = GridOptionsBuilder.from_dataframe(filtered_data)
        gb.configure_default_column(
            editable=False,
            sortable=True,
            filter=True,
            resizable=True,
            wrapHeaderText=True,
            autoHeaderLabel=True,
            autoHeaderTooltip=True,
            autoHeaderCellFilter=True,
            autoHeaderCellRenderer=True,
            autoHeaderHeight=True,
            filterParams={
                'filterOptions': ['equals', 'notEqual', 'contains', 'notContains', 'startsWith', 'endsWith'],
                'defaultOption': 'contains'
            },
            menuTabs=['generalMenuTab', 'filterMenuTab', 'columnsMenuTab']
        )

        # Add advanced chart button in the TICKER_SYMBOL column
        gb.configure_column(
            "TICKER_SYMBOL",
            cellRenderer=JsCode("""
            function(params) {
                return `
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="font-weight: bold;">${params.value}</span>
                        <button onclick="showAdvancedChart('${params.value}')"
                                style="background: linear-gradient(45deg, #2196F3, #21CBF3);
                                       color: white;
                                       border: none;
                                       padding: 4px 12px;
                                       border-radius: 4px;
                                       cursor: pointer;
                                       display: flex;
                                       align-items: center;
                                       gap: 4px;">
                            <span>📈</span>
                            <span>Advanced Chart</span>
                        </button>
                    </div>
                `;
            }
            """)
        )

        grid_options = get_grid_options()
        if quick_search:
            grid_options["quickFilterText"] = quick_search

        final_grid_options = gb.build()
        final_grid_options.update(grid_options)

        response = AgGrid(
            filtered_data,
            gridOptions=final_grid_options,
            enable_enterprise_modules=True,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            width='100%',
            height=600,
            allow_unsafe_jscode=True,
            theme='streamlit',
            enable_quicksearch=True,
            reload_data=True
        )

        selected_rows = response['selected_rows']

        highlight_button = st.button("Highlight Selected Rows")
        if highlight_button:
            # Get tickers and highlight rows via JS
            tickers_to_highlight = [row['TICKER_SYMBOL'] for row in selected_rows if 'TICKER_SYMBOL' in row]
            if tickers_to_highlight:
                st_javascript(
                    code=f"""
                    const tickers = {tickers_to_highlight};
                    const rows = document.querySelectorAll('.ag-center-cols-container .ag-row');
                    rows.forEach(row => {{
                        const tickerCell = row.querySelector("[col-id='TICKER_SYMBOL'] .ag-cell-value");
                        if (tickerCell && tickers.includes(tickerCell.innerText)) {{
                            row.classList.add('highlighted-row');
                        }}
                    }});
                    """,
                    onload=False
                )

        # Add JS for the Advanced Chart Modal
        st.markdown("""
        <script>
        function showAdvancedChart(ticker) {
            const modal = document.createElement('div');
            modal.style.cssText = `
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0,0,0,0.85);
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 1000;
                backdrop-filter: blur(5px);
            `;
        
            const modalContent = document.createElement('div');
            modalContent.style.cssText = `
                width: 95%;
                height: 90%;
                background: #131722;
                border-radius: 12px;
                position: relative;
                padding: 20px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            `;
        
            const closeButton = document.createElement('button');
            closeButton.innerHTML = '✕';
            closeButton.style.cssText = `
                position: absolute;
                top: 20px;
                right: 20px;
                background: rgba(255,255,255,0.1);
                border: none;
                color: white;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
                z-index: 1001;
            `;
            closeButton.onclick = () => document.body.removeChild(modal);
        
            modalContent.appendChild(closeButton);
            modal.appendChild(modalContent);
            document.body.appendChild(modal);
        
            new TradingView.widget({
                "autosize": true,
                "symbol": ticker,
                "interval": "D",
                "timezone": "Etc/UTC",
                "theme": "dark",
                "style": "1",
                "locale": "en",
                "toolbar_bg": "#f1f3f6",
                "enable_publishing": false,
                "withdateranges": true,
                "range": "YTD",
                "allow_symbol_change": true,
                "details": true,
                "hotlist": true,
                "calendar": true,
                "container_id": "tradingview_modal"
            });
        }
        </script>
        """, unsafe_allow_html=True)

        if selected_rows is not None and len(selected_rows) > 0:
            st.subheader("Selected Rows Details")
            etf_objects = [ETF(row) for row in selected_rows]
            for etf_obj in etf_objects:
                st.write(etf_obj)

            # Show TradingView analysis for the first selected ticker, if any
            first_ticker = selected_rows[0]['TICKER_SYMBOL'] if 'TICKER_SYMBOL' in selected_rows[0] else None
            if first_ticker:
                st.subheader(f"TradingView Analysis for {first_ticker}")
                show_tradingview_analysis(first_ticker)


if __name__ == "__main__":
    main()
