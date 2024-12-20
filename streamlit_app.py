import asyncio
from typing import Dict, Optional, Any
from datetime import datetime
import logging
import streamlit as st
from st_aggrid import AgGrid, DataReturnMode, GridUpdateMode, JsCode, GridOptionsBuilder
import pandas as pd
import io
import xlsxwriter
import aiohttp
from functools import lru_cache
import concurrent.futures
from dataclasses import dataclass

st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈"
)

# Custom CSS
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

logging.basicConfig(level=logging.INFO)
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
        self.data = data
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
        'groupSelectsChildren': True,
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
        'suppressAggFuncInHeader': False,
        'suppressColumnVirtualisation': False,
        'suppressRowVirtualisation': False,
        'suppressMenuHide': False,
        'suppressMovableColumns': False,
        'suppressFieldDotNotation': True,
        'suppressCopyRowsToClipboard': False,
        'suppressCopySingleCellRanges': False,
        'suppressMultiRangeSelection': False,
        'suppressParentsInRowNodes': False,
        'suppressTouch': False,
        'animateRows': True,
        'allowContextMenuWithControlKey': True,
        'suppressContextMenu': False,
        'suppressMenuFilterPanel': False,
        'suppressMenuMainPanel': False,
        'suppressMenuColumnPanel': False,
        'enableValue': True,
        'enablePivoting': True,
        'enableRowGroup': True,
        'enableQuickFilter': True,
        'floatingFilter': True,
        'includeRowGroupColumns': True,
        'includeValueColumns': True,
        'includePivotColumns': True,
        'pagination': False,
        'rowModelType': 'clientSide'
    }

def configure_grid(df: pd.DataFrame, group_by_column: Optional[str] = None) -> Dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    custom_css = {
        ".ag-status-bar": {
            "font-size": "16px",
            "font-weight": "bold",
            "color": "#333",
        },
        ".ag-status-bar .ag-status-name-value": {
            "font-size": "16px",
        }
    }

    gb.configure_default_column(
        editable=True,
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
            'filterOptions': [
                'equals', 'notEqual', 'contains',
                'notContains', 'startsWith', 'endsWith'
            ],
            'defaultOption': 'contains'
        },
        menuTabs=['generalMenuTab', 'filterMenuTab', 'columnsMenuTab']
    )

    if group_by_column and group_by_column in df.columns:
        gb.configure_column(group_by_column, rowGroup=True, hide=True)

    gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren=True, header_checkbox=True)
    gb.configure_grid_options(rowHeight=50, paginationPageSize=20, onFirstDataRendered='onFirstDataRendered')

    status_panels = {
        "statusPanels": [
            {"statusPanel": "agTotalAndFilteredRowCountComponent", "align": "left"},
            {"statusPanel": "agTotalRowCountComponent", "align": "center"},
            {"statusPanel": "agFilteredRowCountComponent", "align": "center"},
            {"statusPanel": "agSelectedRowCountComponent", "align": "right"},
            {"statusPanel": "agAggregationComponent", "align": "right"}
        ]
    }

    grid_options = get_grid_options()
    gb.configure_grid_options(
        statusBar=status_panels,
        **grid_options,
        rowStyle={
            'background-color': 'rgba(0, 0, 0, 0.05)',
            'border-radius': '10px',
            'box-shadow': '0px 1px 5px rgba(0, 0, 0, 0.2)',
            'margin-bottom': '5px',
            'padding': '10px'
        },
        headerStyle={
            'background-color': 'rgba(0, 0, 0, 0.1)',
            'border-radius': '10px',
            'box-shadow': '0px 1px 5px rgba(0, 0, 0, 0.2)',
            'padding': '10px'
        },
        sideBar={
            'toolPanels': [
                {
                    'id': 'columns',
                    'labelDefault': 'Columns',
                    'labelKey': 'columns',
                    'iconKey': 'columns',
                    'toolPanel': 'agColumnsToolPanel'
                },
                {
                    'id': 'filters',
                    'labelDefault': 'Filters',
                    'labelKey': 'filters',
                    'iconKey': 'filter',
                    'toolPanel': 'agFiltersToolPanel'
                }
            ],
            'defaultToolPanel': ''
        },
    )

    # Button renderer for ACTION column
    button_renderer = JsCode('''
        class ButtonRenderer {
            init(params) {
                this.eGui = document.createElement('button');
                this.eGui.innerHTML = '📈 View Chart';
                this.eGui.style.cssText = `
                    background: linear-gradient(45deg, #2196F3, #21CBF3);
                    color: white;
                    border: none;
                    padding: 5px 15px;
                    border-radius: 5px;
                    cursor: pointer;
                    font-weight: bold;
                    transition: all 0.3s ease;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.2);
                `;
                this.eGui.addEventListener('mouseover', () => {
                    this.eGui.style.transform = 'translateY(-2px)';
                    this.eGui.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
                });
                this.eGui.addEventListener('mouseout', () => {
                    this.eGui.style.transform = 'translateY(0)';
                    this.eGui.style.boxShadow = '0 2px 5px rgba(0,0,0,0.2)';
                });
                this.eGui.addEventListener('click', () => {
                    const modal = document.createElement('div');
                    modal.style.cssText = `
                        position: fixed;
                        top: 0;
                        left: 0;
                        width: 100%;
                        height: 100%;
                        background: rgba(0,0,0,0.8);
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        z-index: 1000;
                        backdrop-filter: blur(5px);
                    `;
                    const modalContent = document.createElement('div');
                    modalContent.style.cssText = `
                        background: #1E1E1E;
                        padding: 20px;
                        border-radius: 15px;
                        width: 90%;
                        height: 90%;
                        position: relative;
                        box-shadow: 0 10px 25px rgba(0,0,0,0.5);
                        border: 1px solid rgba(255,255,255,0.1);
                    `;
                    const closeBtn = document.createElement('button');
                    closeBtn.innerHTML = '✕';
                    closeBtn.style.cssText = `
                        position: absolute;
                        top: 15px;
                        right: 15px;
                        background: rgba(255,255,255,0.1);
                        color: #fff;
                        border: none;
                        border-radius: 50%;
                        width: 30px;
                        height: 30px;
                        cursor: pointer;
                        font-size: 16px;
                        transition: all 0.3s ease;
                    `;
                    closeBtn.addEventListener('mouseover', () => {
                        closeBtn.style.background = 'rgba(255,255,255,0.2)';
                        closeBtn.style.transform = 'scale(1.1)';
                    });
                    closeBtn.addEventListener('mouseout', () => {
                        closeBtn.style.background = 'rgba(255,255,255,0.1)';
                        closeBtn.style.transform = 'scale(1)';
                    });
                    closeBtn.onclick = () => {
                        modal.style.opacity = '0';
                        setTimeout(() => document.body.removeChild(modal), 300);
                    };

                    modalContent.appendChild(closeBtn);
                    modal.appendChild(modalContent);
                    document.body.appendChild(modal);

                    const ticker = params.data.TICKER_SYMBOL;
                    const widgetContainer = document.createElement('div');
                    widgetContainer.className = 'tradingview-widget-container';
                    widgetContainer.style.cssText = `
                        width: 100%;
                        height: calc(100% - 40px);
                    `;
                    const widgetDiv = document.createElement('div');
                    widgetDiv.className = 'tradingview-widget-container__widget';
                    widgetDiv.style.cssText = `
                        width: 100%;
                        height: 100%;
                    `;
                    widgetContainer.appendChild(widgetDiv);
                    modalContent.appendChild(widgetContainer);

                    const script = document.createElement('script');
                    script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';
                    script.async = true;
                    script.innerHTML = JSON.stringify({
                        "autosize": true,
                        "symbol": ticker,
                        "interval": "D",
                        "timezone": "Etc/UTC",
                        "theme": "dark",
                        "style": "1",
                        "locale": "en",
                        "enable_publishing": false,
                        "allow_symbol_change": true,
                        "calendar": true,
                        "support_host": "https://www.tradingview.com",
                        "width": "100%",
                        "height": "100%",
                        "save_image": true,
                        "hideideas": true,
                        "studies": [
                            "MASimple@tv-basicstudies",
                            "RSI@tv-basicstudies",
                            "MACD@tv-basicstudies",
                            "BB@tv-basicstudies"
                        ],
                        "show_popup_button": true,
                        "popup_width": "1000",
                        "popup_height": "650",
                        "container_id": "tradingview_chart"
                    });
                    widgetContainer.appendChild(script);

                    modal.style.opacity = '0';
                    modal.style.transition = 'opacity 0.3s ease';
                    setTimeout(() => modal.style.opacity = '1', 10);
                });
            }
            getGui() { return this.eGui; }
        }
    ''')
    gb.configure_column('ACTION', headerName="CHART", cellRenderer=button_renderer)

    return gb.build(), custom_css

def show_tradingview_analysis(ticker: str):
    tab1, tab2, tab3 = st.tabs(["📈 Chart", "📊 Technical Analysis", "🏢 Profile"])
    with tab1:
        st.components.v1.html(create_enhanced_tradingview_chart(ticker, "tradingview_chart"), height=800)
    with tab2:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.components.v1.html(create_tradingview_technical_analysis(ticker), height=500)
        with col2:
            st.markdown("<div style='background: rgba(255,255,255,0.1); padding:1rem; border-radius:8px;'><h4>Trading Signals</h4><p>Coming soon...</p></div>", unsafe_allow_html=True)
    with tab3:
        st.components.v1.html(create_tradingview_company_profile(ticker), height=450)


def main() -> None:
    st.title("📈 ETF Explorer Pro")
    st.markdown("Explore ETFs with infinite scrolling, custom CSS, JS interactivity, filtering, exporting, and TradingView integration.")

    with st.spinner("Loading ETF data..."):
        etf_data = load_data()
    if etf_data.empty:
        st.error("No data available.")
        return

    col1, col2 = st.columns([1, 9])

    with col1:
        issuers = sorted(etf_data['ETF_ISSUER'].dropna().unique().tolist())
        selected_issuer = st.selectbox("Filter by ETF Issuer", ["All"] + issuers, index=0)

        asset_classes = sorted(etf_data['ASSET_CLASS'].dropna().unique().tolist())
        selected_asset_class = st.selectbox("Filter by Asset Class", ["All"] + asset_classes, index=0)

        numeric_aum = etf_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('M', '')
        numeric_aum = pd.to_numeric(numeric_aum, errors='coerce')
        max_aum = int(numeric_aum.max()) if numeric_aum.notnull().any() else 0
        min_aum = st.slider("Min AUM ($M)", min_value=0, max_value=max_aum, value=0)

    filtered_data = etf_data.copy()
    if selected_issuer != "All":
        filtered_data = filtered_data[filtered_data['ETF_ISSUER'] == selected_issuer]
    if selected_asset_class != "All":
        filtered_data = filtered_data[filtered_data['ASSET_CLASS'] == selected_asset_class]

    numeric_filtered_aum = filtered_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('M', '')
    numeric_filtered_aum = pd.to_numeric(numeric_filtered_aum, errors='coerce')
    filtered_data = filtered_data[numeric_filtered_aum >= min_aum]

    with col2:
        quick_search = st.text_input("Global Quick Search", value="", help="Type to filter all columns globally")
        final_grid_options, custom_css = configure_grid(filtered_data, group_by_column=None)
        if quick_search:
            final_grid_options["quickFilterText"] = quick_search

        response = AgGrid(
            filtered_data,
            gridOptions=final_grid_options,
            enable_enterprise_modules=True,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            width='100%',
            height=1500,
            allow_unsafe_jscode=True,
            theme='streamlit',
            enable_quicksearch=True,
            reload_data=True
        )

        # Trigger modal for export options
        if st.button("Export Data"):
            st.session_state.show_modal = True

        if st.session_state.get("show_modal", False):
            with st.modal("Export Options"):
                st.markdown("### Select Export Format")
                export_format = st.radio("Choose file type", ["CSV", "Excel"])
                confirm_export = st.button("Confirm Export")

                if confirm_export:
                    if export_format == "CSV":
                        # Export as CSV
                        st.download_button(
                            label="Download CSV",
                            data=response['data'].to_csv(index=False).encode('utf-8'),
                            file_name="exported_data.csv",
                            mime="text/csv"
                        )
                    elif export_format == "Excel":
                        # Export as Excel
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            pd.DataFrame(response['data']).to_excel(writer, index=False, sheet_name='Sheet1')
                        output.seek(0)
                        st.download_button(
                            label="Download Excel",
                            data=output,
                            file_name="exported_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    st.session_state.show_modal = False

if __name__ == "__main__":
    main()

    
