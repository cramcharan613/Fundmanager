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

st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ETFData:
    """Data class for ETF information."""
    ticker: str
    issuer: str
    description: str
    asset_class: str
    inception_date: str
    aum: float
    expense_ratio: float
    holdings: int

class CachedETFDataFetcher:
    """Enhanced ETF data fetcher with caching and parallel processing."""
    def __init__(self):
        self.stockanalysis_url = (
            "https://api.stockanalysis.com/api/screener/e/bd/"
            "issuer+n+assetClass+inceptionDate+exchange+etfLeverage+"
            "aum+close+holdings+price+cusip+isin+etfCategory+"
            "expenseRatio+etfIndex+etfRegion+etfCountry+optionable.json"
        )

    @lru_cache(maxsize=1)
    async def fetch_data(self) -> pd.DataFrame:
        """Fetch ETF data with caching."""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.stockanalysis_url) as resp:
                raw_data = await resp.json()
                return self._process_data(raw_data)

    def _process_data(self, raw_data: Dict) -> pd.DataFrame:
        """Process raw data with parallel processing."""
        if not raw_data or 'data' not in raw_data:
            return pd.DataFrame()

        raw_entries = raw_data['data']['data']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            processed_data = list(executor.map(self._process_etf_entry, raw_entries.items()))

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
        """Enhance dataframe with numeric formatting."""
        # Format numeric columns
        # CURRENT_PRICE, AUM in millions, EXPENSE_RATIO as percentage
        df['CURRENT_PRICE'] = pd.to_numeric(df['CURRENT_PRICE'], errors='coerce')
        df['CURRENT_PRICE'] = df['CURRENT_PRICE'].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else '')

        df['ASSETS_UNDER_MANAGEMENT'] = pd.to_numeric(df['ASSETS_UNDER_MANAGEMENT'], errors='coerce')
        df['ASSETS_UNDER_MANAGEMENT'] = df['ASSETS_UNDER_MANAGEMENT'].apply(lambda x: f"${x:,.2f}M" if pd.notnull(x) else '')

        df['EXPENSE_RATIO'] = pd.to_numeric(df['EXPENSE_RATIO'], errors='coerce')
        df['EXPENSE_RATIO'] = df['EXPENSE_RATIO'].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else '')

        # Move CUSIP to front
        if 'CUSIP' in df.columns:
            cols = ['CUSIP'] + [c for c in df.columns if c != 'CUSIP']
            df = df[cols]

        return df

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Load ETF data with caching."""
    fetcher = CachedETFDataFetcher()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetcher.fetch_data())

# Custom class for displaying selected rows as HTML
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
        'pagination': False,  # no pagination for infinite scrolling
        'rowModelType': 'clientSide'
    }

def main() -> None:
    st.title("📈 ETF Explorer Pro")

    st.markdown("""
    Explore ETFs with interactive filtering, grouping, pivoting, infinite scrolling,
    and native Streamlit dark/light mode support.
    """)

    with st.spinner("Loading ETF data..."):
        etf_data = load_data()

    if etf_data.empty:
        st.error("No data available. Please try again later.")
        return

    # Two columns layout: left (10%) for filters and exports, right (90%) for quick search and grid
    col1, col2 = st.columns([1, 9])

    with col1:
        # Filters
        issuers = sorted(etf_data['ETF_ISSUER'].dropna().unique().tolist())
        selected_issuer = st.selectbox("Filter by ETF Issuer", ["All"] + issuers, index=0)

        asset_classes = sorted(etf_data['ASSET_CLASS'].dropna().unique().tolist())
        selected_asset_class = st.selectbox("Filter by Asset Class", ["All"] + asset_classes, index=0)

        # AUM filter
        numeric_aum = etf_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('M','')
        numeric_aum = pd.to_numeric(numeric_aum, errors='coerce')
        max_aum = int(numeric_aum.max()) if numeric_aum.notnull().any() else 0
        min_aum = st.slider("Min AUM ($M)", min_value=0, max_value=max_aum, value=0)

        # Export Format
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

    # Filter data based on user selections
    filtered_data = etf_data.copy()
    if selected_issuer != "All":
        filtered_data = filtered_data[filtered_data['ETF_ISSUER'] == selected_issuer]

    if selected_asset_class != "All":
        filtered_data = filtered_data[filtered_data['ASSET_CLASS'] == selected_asset_class]

    numeric_filtered_aum = filtered_data['ASSETS_UNDER_MANAGEMENT'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('M','')
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

        # No grouping by issuer requested at this moment, but if you want:
        # If you want to group by issuer:
        # group_by_issuer = st.checkbox("Group by ETF Issuer", value=False) # Moved earlier if needed
        # if group_by_issuer and "ETF_ISSUER" in filtered_data.columns:
        #     gb.configure_column("ETF_ISSUER", rowGroup=True, hide=True)

        grid_options = get_grid_options()
        if quick_search:
            grid_options["quickFilterText"] = quick_search

        gb.configure_grid_options(**grid_options)

        response = AgGrid(
            filtered_data,
            gridOptions=gb.build(),
            enable_enterprise_modules=True,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            width='100%',
            height=800,
            allow_unsafe_jscode=True,
            theme='streamlit',
            enable_quicksearch=True,
            reload_data=True
        )

        selected_rows = response['selected_rows']

        # Check if we have selected rows safely
        if selected_rows is not None and len(selected_rows) > 0:
            st.subheader("Selected Rows Details")
            etf_objects = [ETF(row) for row in selected_rows]
            for etf_obj in etf_objects:
                st.write(etf_obj)

if __name__ == "__main__":
    main()
