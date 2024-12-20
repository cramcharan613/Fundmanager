import asyncio
from typing import Dict, Optional
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

# Set basic page config
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

class ETFDataFetcher:
    def __init__(self):
        self.stockanalysis_url = (
            "https://api.stockanalysis.com/api/screener/e/bd/"
            "issuer+n+assetClass+inceptionDate+exchange+etfLeverage+"
            "aum+close+holdings+price+cusip+isin+etfCategory+"
            "expenseRatio+etfIndex+etfRegion+etfCountry+optionable.json"
        )

    def preprocess_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = [
            'CURRENT_PRICE', 'CLOSING_PRICE',
            'ASSETS_UNDER_MANAGEMENT', 'EXPENSE_RATIO'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col in ['CURRENT_PRICE', 'CLOSING_PRICE', 'ASSETS_UNDER_MANAGEMENT']:
                    df[col] = df[col].apply(
                        lambda x: f"${x:,.2f}" if pd.notnull(x) else ''
                    )
                elif col == 'EXPENSE_RATIO':
                    df[col] = df[col].apply(
                        lambda x: f"{x:.2%}" if pd.notnull(x) else ''
                    )
        return df

    async def fetch_stockanalysis_data(self) -> pd.DataFrame:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.stockanalysis_url) as resp:
                    resp.raise_for_status()
                    raw_data = await resp.json()

            if raw_data and 'data' in raw_data:
                raw = raw_data.get("data")
                df = (
                    pd.DataFrame()
                    .from_dict(raw.get("data", {}))
                    .T.reset_index()
                    .rename(columns=str.upper)
                    .rename(
                        columns={
                            "INDEX": "TICKER_SYMBOL",
                            "ISSUER": "ETF_ISSUER",
                            "N": "ETF_DESCRIPTION",
                            "ASSETCLASS": "ASSET_CLASS",
                            "INCEPTIONDATE": "INCEPTION_DATE",
                            "EXCHANGE": "LISTED_EXCHANGE",
                            "ETFLEVERAGE": "LEVERAGE",
                            "AUM": "ASSETS_UNDER_MANAGEMENT",
                            "CLOSE": "CLOSING_PRICE",
                            "HOLDINGS": "NUMBER_OF_HOLDINGS",
                            "PRICE": "CURRENT_PRICE",
                            "ETFCATEGORY": "ETF_CATEGORY",
                            "EXPENSERATIO": "EXPENSE_RATIO",
                            "ETFINDEX": "TRACKING_INDEX",
                            "ETFREGION": "GEOGRAPHIC_REGION",
                            "ETFCOUNTRY": "COUNTRY_FOCUS",
                            "OPTIONABLE": "HAS_OPTIONS",
                            "CUSIP": "CUSIP"
                        }
                    )
                )

                df = self.preprocess_numeric_data(df)

                # Move CUSIP to the front if present
                if 'CUSIP' in df.columns:
                    cols = ['CUSIP'] + [c for c in df.columns if c != 'CUSIP']
                    df = df[cols]

                return df
            logger.error("Failed to fetch or process Stock Analysis data")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching Stock Analysis data: {str(e)}")
            return pd.DataFrame()

@st.cache_data()
def load_data() -> pd.DataFrame:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    fetcher = ETFDataFetcher()
    return loop.run_until_complete(fetcher.fetch_stockanalysis_data())

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
        'animateRows': True,
        'allowContextMenuWithControlKey': True,
        'pagination': True,
        'paginationPageSize': 20
    }

def configure_grid(df: pd.DataFrame, group_by_column: Optional[str] = None) -> Dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    custom_css = {
        ".ag-status-bar": {
            "font-size": "14px",
            "font-weight": "500",
            "color": "var(--text-color)"
        },
        ".ag-status-bar .ag-status-name-value": {
            "font-size": "14px",
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
            'filterOptions': ['equals', 'notEqual', 'contains', 'notContains', 'startsWith', 'endsWith'],
            'defaultOption': 'contains'
        },
        menuTabs=['generalMenuTab', 'filterMenuTab', 'columnsMenuTab']
    )

    if group_by_column and group_by_column in df.columns:
        gb.configure_column(group_by_column, rowGroup=True, hide=True)

    gb.configure_selection(
        'multiple',
        use_checkbox=True,
        groupSelectsChildren=True,
        header_checkbox=True
    )
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)

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
        **grid_options
    )

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
                        display: flex;
                        align-items: center;
                        justify-content: center;
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
                    modalContent.appendChild(closeBtn);
                    modalContent.appendChild(widgetContainer);
                    modal.appendChild(modalContent);
                    document.body.appendChild(modal);

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
            getGui() {
                return this.eGui;
            }
        }
    ''')
    gb.configure_column('ACTION', headerName="CHART", cellRenderer=button_renderer)

    return gb.build(), custom_css

def display_summary_stats(df: pd.DataFrame) -> None:
    try:
        total_etfs = len(df)
        total_aum = pd.to_numeric(
            df['ASSETS_UNDER_MANAGEMENT']
            .str.replace('$', '', regex=False)
            .str.replace(',', '', regex=False),
            errors='coerce'
        ).sum()
        avg_expense = pd.to_numeric(
            df['EXPENSE_RATIO'].str.rstrip('%'),
            errors='coerce'
        ).mean()

        st.toast(f"📊 Total ETFs: **{total_etfs:,}**", icon="📈")
        time.sleep(0.5)
        st.toast(f"💰 Total AUM: **${total_aum:,.2f}B**", icon="💵")
        time.sleep(0.5)
        st.toast(f"📉 Avg Expense Ratio: **{avg_expense:.2f}%**", icon="🧾")
    except Exception as e:
        st.error(f"Failed to calculate and display summary stats: {str(e)}")
        logger.error(f"Summary stats error: {str(e)}")

def export_data(df: pd.DataFrame, format: str = 'csv') -> tuple[Optional[bytes], Optional[str], Optional[str]]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        if format.lower() == 'csv':
            buffer = io.BytesIO()
            df.to_csv(buffer, index=False, encoding='utf-8')
            mime_type = "text/csv"
            file_extension = "csv"
        elif format.lower() == 'excel':
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='ETF_Data')
                worksheet = writer.sheets['ETF_Data']
                for i, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.set_column(i, i, max_length)
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            file_extension = "xlsx"
        else:
            raise ValueError(f"Unsupported export format: {format}")
        buffer.seek(0)
        bytes_data = buffer.getvalue()
        filename = f"etf_data_{timestamp}.{file_extension}"
        return bytes_data, filename, mime_type
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        st.error(f"Failed to export data: {str(e)}")
        return None, None, None

# Dark/Light mode CSS
light_mode_css = """
:root {
    --bg-color: #f0f2f5;
    --text-color: #333;
    --header-bg: #f0f2f5;
}
body {
    background: var(--bg-color);
    color: var(--text-color);
}
"""

dark_mode_css = """
:root {
    --bg-color: #121212;
    --text-color: #dddddd;
    --header-bg: #1f1f1f;
}
body {
    background: var(--bg-color) !important;
    color: var(--text-color) !important;
}
.ag-root-wrapper, .ag-header, .ag-header-container, .ag-status-bar {
    background: var(--header-bg) !important;
    color: var(--text-color) !important;
}
"""

def display_export_section(df: pd.DataFrame) -> None:
    export_format = st.selectbox(
        "Export Format",
        options=["CSV", "Excel"],
        key="export_format"
    )
    if export_format:
        bytes_data, filename, mime_type = export_data(df, export_format.lower())
        if bytes_data and filename and mime_type:
            st.download_button(
                label=f"📥 Download {export_format}",
                data=bytes_data,
                file_name=filename,
                mime=mime_type,
                key=f"download_{export_format.lower()}",
                help=f"Click to download the ETF data as {export_format} file"
            )

def main() -> None:
    # Dark/Light mode toggle
    dark_mode = st.sidebar.checkbox("Dark Mode", value=False)

    # Apply chosen theme CSS
    if dark_mode:
        st.markdown(f"<style>{dark_mode_css}</style>", unsafe_allow_html=True)
    else:
        st.markdown(f"<style>{light_mode_css}</style>", unsafe_allow_html=True)

    st.title("📈 ETF Explorer Pro")
    st.markdown("""
    **A modern, interactive platform for exploring ETFs**:
    - Toggle dark/light mode using the sidebar.
    - Filter, pivot, and group data to your liking.
    - Export filtered results.
    """)

    try:
        with st.spinner("Loading ETF data..."):
            start_time = time.time()
            etf_data = load_data()
            load_time = time.time() - start_time
            st.success(f"Data loaded in {load_time:.2f} seconds")
    except Exception as e:
        st.error(f"Failed to load ETF data: {str(e)}")
        return

    if etf_data.empty:
        st.error("No data available.")
        return

    # Layout with 2 columns: left col = 10%, right col = 90%
    col1, col2 = st.columns([1,9])

    # Left column: All dropdowns and checkboxes
    with col1:
        unique_issuers = etf_data['ETF_ISSUER'].dropna().unique()
        selected_issuer = st.selectbox(
            "Filter by ETF Issuer",
            options=["All"] + sorted(unique_issuers.tolist()),
            index=0
        )
        group_by_issuer = st.checkbox("Group by ETF Issuer", value=False)

        # Export options on left as well
        display_export_section(etf_data)

    # Right column: Quick search above the grid
    filtered_data = etf_data[etf_data['ETF_ISSUER'] == selected_issuer] if selected_issuer != "All" else etf_data

    display_summary_stats(filtered_data)

    quick_search = st.text_input("Global Quick Search", value="", help="Type to filter all columns globally")

    try:
        grid_options, custom_css = configure_grid(
            filtered_data,
            group_by_column="ETF_ISSUER" if group_by_issuer else None
        )
        if quick_search:
            grid_options["quickFilterText"] = quick_search

        response = AgGrid(
            filtered_data,
            gridOptions=grid_options,
            enable_enterprise_modules=True,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            width='100%',
            height=800,
            allow_unsafe_jscode=True,
            theme='streamlit',
            enable_quicksearch=True,
            reload_data=True,
            custom_css=custom_css
        )

        selected_rows = response['selected_rows']

        if selected_rows:
            st.subheader("Selected Rows Details")
            sel_df = pd.DataFrame(selected_rows)
            st.dataframe(sel_df)

    except Exception as e:
        st.error(f"Error displaying grid: {str(e)}")
        logger.error(f"Grid error: {str(e)}")

if __name__ == "__main__":
    main()
