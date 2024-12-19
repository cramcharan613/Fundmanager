```python
import asyncio
from typing import Dict, Optional
from datetime import datetime
import logging
from dotenv import load_dotenv
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import pandas as pd
import json
import openpyxl
import io
import xlsxwriter
import os
import time
import re
import aiohttp
from playwright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

@st.cache_data()
def ensure_playwright_setup() -> bool:
    if not os.path.exists('/usr/bin/google-chrome'):
        os.system('playwright install-deps')
    os.system('playwright install')
    return True

ensure_playwright_setup()

class ETFDataFetcher:
    def __init__(self):
        self.stockanalysis_url = (
            "https://api.stockanalysis.com/api/screener/e/bd/"
            "issuer+n+assetClass+inceptionDate+exchange+etfLeverage+"
            "aum+close+holdings+price+cusip+isin+etfCategory+"
            "expenseRatio+etfIndex+etfRegion+etfCountry+optionable.json"
        )
        self.etfdb_base_url = "https://etfdb.com"
        self.etfdb_api_url = "https://etfdb.com/api/screener"
        self.etfdb_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
        }

    async def get_dynamic_headers(self, url: str) -> Dict:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox']
                )
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    locale='en-US'
                )
                page = await context.new_page()
                headers = {}

                async def handle_response(response):
                    if response.url == url:
                        h = await response.all_headers()
                        for k, v in h.items():
                            if v is not None and v != '':
                                headers[k] = v
                        headers.update({
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Sec-Fetch-User': '?1',
                            'Cache-Control': 'max-age=0'
                        })

                page.on('response', handle_response)
                try:
                    await page.goto(url, wait_until='networkidle', timeout=30000)
                    await page.wait_for_load_state('domcontentloaded')
                    await page.wait_for_load_state('networkidle')
                    await page.wait_for_timeout(2000)
                except:
                    pass
                await context.close()
                await browser.close()
                if not headers:
                    return {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5'
                    }
                return headers
        except:
            return {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            }

    async def make_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict] = None,
        payload: Optional[Dict] = None
    ) -> Optional[dict]:
        if headers is None:
            headers = await self.get_dynamic_headers(url)

        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        args=['--no-sandbox', '--disable-setuid-sandbox']
                    )
                    context = await browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent=headers.get('User-Agent', ''),
                        locale='en-US'
                    )
                    page = await context.new_page()
                    await page.set_extra_http_headers(headers)

                    base_url_match = re.match(r"(https://[^/]+)", url)
                    base_url = base_url_match.group(1) if base_url_match else "https://etfdb.com"

                    await page.goto(base_url, wait_until='networkidle', timeout=30000)

                    if method.upper() == "GET":
                        response_data = await page.evaluate(f'''
                            fetch("{url}", {{
                                method: "GET",
                                headers: {json.dumps(headers)}
                            }}).then(response => response.text())
                        ''')
                    else:
                        response_data = await page.evaluate(f'''
                            fetch("{url}", {{
                                method: "{method.upper()}",
                                headers: {json.dumps(headers)},
                                body: {json.dumps(payload) if payload else "null"}
                            }}).then(response => response.text())
                        ''')

                    await context.close()
                    await browser.close()

                    if response_data:
                        try:
                            return json.loads(response_data)
                        except json.JSONDecodeError:
                            logger.error(f"Error fetching: Response not valid JSON, received: {response_data[:100]}")
                            return None
            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Max retries reached for URL: {url}")
                    return None
        return None

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
            headers = await self.get_dynamic_headers(self.stockanalysis_url)
            data = await self.make_request(self.stockanalysis_url, headers=headers)
            if data and 'data' in data:
                raw = data.get("data")
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
                        }
                    )
                )
                return self.preprocess_numeric_data(df)
            logger.error("Failed to fetch or process Stock Analysis data")
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    async def fetch_etfdb_data(self) -> pd.DataFrame:
        import json
        all_data = []
        max_pages = 56

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox']
                )
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
                    locale='en-US'
                )
                page = await context.new_page()
                await page.goto(self.etfdb_base_url, wait_until='networkidle', timeout=30000)

                # Use the specified etfdb_headers for all requests
                headers = self.etfdb_headers

                for page_num in range(1, max_pages + 1):
                    try:
                        payload = {"active_or_passive": "Active", "page": page_num}
                        # Execute the fetch call in the page's context
                        result = await page.evaluate(f'''
                            fetch("{self.etfdb_api_url}", {{
                                method: "POST",
                                headers: {json.dumps(headers)},
                                body: {json.dumps(payload)}
                            }})
                            .then(response => response.text())
                            .then(text => {{
                                try {{
                                    return JSON.parse(text);
                                }} catch(e) {{
                                    return null;
                                }}
                            }})
                        ''')
                        if result and isinstance(result, dict) and 'data' in result:
                            all_data.extend(result['data'])

                        # Add a small sleep between requests to avoid overwhelming the server
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        logger.error(f"Error fetching page {page_num}: {str(e)}")
                        continue

                await context.close()
                await browser.close()
            if all_data:
                etf_df = pd.DataFrame(all_data)
                etf_df.loc[:, "symbol"] = etf_df["mobile_title"].apply(
                    lambda x: x.split(" - ")[0] if isinstance(x, str) else ""
                )
                etf_df["Actively Managed"] = "YES"
                return etf_df[["symbol", "Actively Managed"]]
        except:
            pass
        return pd.DataFrame(columns=["symbol", "Actively Managed"])

    def merge_data(self, stockanalysis_df: pd.DataFrame, etfdb_df: pd.DataFrame) -> pd.DataFrame:
        merged_df = pd.merge(
            stockanalysis_df,
            etfdb_df,
            left_on="TICKER_SYMBOL",
            right_on="symbol",
            how="left"
        )
        merged_df["Actively Managed"] = merged_df["Actively Managed"].fillna("NO")
        merged_df = merged_df.drop("symbol", axis=1)
        if 'CURRENT_PRICE' in merged_df.columns and 'CLOSING_PRICE' in merged_df.columns:
            current_price = pd.to_numeric(
                merged_df['CURRENT_PRICE'].str.replace(r'[^0-9\.]', '', regex=True)
            )
            closing_price = pd.to_numeric(
                merged_df['CLOSING_PRICE'].str.replace(r'[^0-9\.]', '', regex=True)
            )
            merged_df['PRICE_CHANGE'] = current_price - closing_price
            merged_df['PRICE_CHANGE_PCT'] = ((current_price - closing_price) / closing_price) * 100
            merged_df['PRICE_CHANGE'] = merged_df['PRICE_CHANGE'].apply(
                lambda x: f"${x:,.2f}" if pd.notnull(x) else ''
            )
            merged_df['PRICE_CHANGE_PCT'] = merged_df['PRICE_CHANGE_PCT'].apply(
                lambda x: f"{x:.2f}%" if pd.notnull(x) else ''
            )
        return merged_df

    async def fetch_and_combine_data(self) -> pd.DataFrame:
        stockanalysis_df = await self.fetch_stockanalysis_data()
        etfdb_df = await self.fetch_etfdb_data()
        return self.merge_data(stockanalysis_df, etfdb_df)


@st.cache_data()
def load_data_sync() -> pd.DataFrame:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    fetcher = ETFDataFetcher()
    return loop.run_until_complete(fetcher.fetch_and_combine_data())

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
    }

def configure_grid(df: pd.DataFrame, group_by_column: Optional[str] = None) -> Dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    custom_css = {
        ".ag-status-bar": {
            "font-size": "16px",
            "font-weight": "bold",
            "color": "#333"
        },
        ".ag-status-bar .ag-status-name-value": {
            "font-size": "16px"
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
    gb.configure_selection(
        'multiple',
        use_checkbox=True,
        groupSelectsChildren=True,
        header_checkbox=True
    )
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=100)
    gb.configure_side_bar(filters_panel=True, columns_panel=True)
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
        rowHeight=50,
        paginationPageSize=20,
        onFirstDataRendered='onFirstDataRendered'
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
        active_etfs = (df['Actively Managed'] == 'YES').sum()
        st.toast(f"📊 Total ETFs: **{total_etfs:,}**", icon="📈")
        time.sleep(3)
        st.toast(f"💰 Total AUM: **${total_aum:,.2f}B**", icon="💵")
        time.sleep(3)
        st.toast(f"📉 Avg Expense Ratio: **{avg_expense:.2f}%**", icon="🧾")
        time.sleep(3)
        st.toast(f"🔍 Active ETFs: **{active_etfs:,}**", icon="✅")
    except Exception as e:
        st.error(f"Failed to calculate and display summary stats: {str(e)}")
        logger.error(f"Summary stats error: {str(e)}")

def export_data(df: pd.DataFrame, format: str = 'csv') -> tuple[bytes, str, str]:
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

def display_export_section(df: pd.DataFrame) -> None:
    st.subheader("Export Options")
    col1, col2 = st.columns([2, 3])
    with col1:
        export_format = st.selectbox(
            "Choose Export Format",
            options=["CSV", "Excel"],
            key="export_format"
        )
    with col2:
        if export_format:
            bytes_data, filename, mime_type = export_data(df, export_format.lower())
            if bytes_data and filename and mime_type:
                st.download_button(
                    label=f"📥 Download as {export_format}",
                    data=bytes_data,
                    file_name=filename,
                    mime=mime_type,
                    key=f"download_{export_format.lower()}",
                    help=f"Click to download the ETF data as {export_format} file"
                )

def main() -> None:
    st.set_page_config(
        layout="wide",
        page_title="ETF Explorer Pro",
        page_icon="📈",
        initial_sidebar_state="expanded"
    )
    st.title("📈 ETF Explorer Pro")
    st.markdown("""
    Comprehensive ETF analysis platform with advanced filtering, visualization,
    and real-time data.
    """)
    try:
        with st.spinner("Loading ETF data..."):
            start_time = time.time()
            etf_data = load_data_sync()
            load_time = time.time() - start_time
            st.success(f"Data loaded successfully in {load_time:.2f} seconds")
    except Exception as e:
        st.error(f"Failed to load ETF data: {str(e)}")
        logger.error(f"Data loading error: {str(e)}")
        return
    if etf_data is not None and not etf_data.empty:
        st.subheader("Filters")
        col1, col2 = st.columns(2)
        with col1:
            unique_issuers = etf_data['ETF_ISSUER'].dropna().unique()
            selected_issuer = st.selectbox(
                "Filter by ETF Issuer",
                options=["All"] + sorted(unique_issuers.tolist()),
                index=0
            )
        with col2:
            group_by_issuer = st.checkbox("Group by ETF Issuer", value=False)
        filtered_data = (
            etf_data[etf_data['ETF_ISSUER'] == selected_issuer]
            if selected_issuer != "All" else etf_data
        )
        display_summary_stats(filtered_data)
        display_export_section(filtered_data)
        st.subheader("ETF Data Grid")
        try:
            grid_options, custom_css = configure_grid(
                filtered_data,
                group_by_column="ETF_ISSUER" if group_by_issuer else None
            )
            AgGrid(
                filtered_data,
                gridOptions=grid_options,
                enable_enterprise_modules=True,
                update_mode='NO_UPDATE',
                data_return_mode='filtered',
                fit_columns_on_grid_load=True,
                width='100%',
                height=800,
                allow_unsafe_jscode=True,
                theme='streamlit',
                enable_quicksearch=True,
                reload_data=True,
                custom_css=custom_css
            )
        except Exception as e:
            st.error(f"Error displaying grid: {str(e)}")
            logger.error(f"Grid error: {str(e)}")

if __name__ == "__main__":
    main()
```
