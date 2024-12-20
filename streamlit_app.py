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
:root {
    --primary-color: #2196F3;
    --secondary-color: #21CBF3;
    --background-color: var(--background-color);
    --text-color: var(--text-color);
}

/* Dark mode specific styles */
@media (prefers-color-scheme: dark) {
    :root {
        --background-color: #1E1E1E;
        --text-color: #FFFFFF;
    }
}

body {
    font-family: "Inter", sans-serif;
    margin: 0;
    padding: 0;
    background: var(--background-color);
    color: var(--text-color);
}

.stApp {
    padding: 1rem;
}

.custom-card {
    background: rgba(255, 255, 255, 0.05);
    border-radius: 10px;
    padding: 1rem;
    margin: 1rem 0;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.1);
}

.metric-card {
    background: linear-gradient(45deg, var(--primary-color), var(--secondary-color));
    color: white;
    padding: 1rem;
    border-radius: 10px;
    text-align: center;
    transition: transform 0.3s ease;
}

.metric-card:hover {
    transform: translateY(-5px);
}

/* Enhanced button styles */
.stButton>button {
    background: linear-gradient(45deg, var(--primary-color), var(--secondary-color));
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

/* Enhanced grid styles */
.ag-theme-streamlit {
    --ag-header-background-color: var(--secondary-background-color);
    --ag-odd-row-background-color: var(--background-color);
    --ag-row-hover-color: rgba(33, 150, 243, 0.1);
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
    
    # Enhanced cell styling
    cell_style_jscode = JsCode("""
    function(params) {
        if (params.column.colId === 'EXPENSE_RATIO') {
            const value = parseFloat(params.value);
            if (value > 1.0) {
                return {
                    'color': '#FF4136',
                    'font-weight': 'bold',
                    'background-color': 'rgba(255, 65, 54, 0.1)'
                };
            } else if (value < 0.3) {
                return {
                    'color': '#2ECC40',
                    'font-weight': 'bold',
                    'background-color': 'rgba(46, 204, 64, 0.1)'
                };
            }
        }
        if (params.column.colId === 'ASSETS_UNDER_MANAGEMENT') {
            const value = parseFloat(params.value.replace(/[^0-9.-]+/g,""));
            if (value > 10000) {
                return {
                    'color': '#0074D9',
                    'font-weight': 'bold',
                    'background-color': 'rgba(0, 116, 217, 0.1)'
                };
            }
        }
        return null;
    }
    """)

    # Enhanced tooltips
    tooltip_jscode = JsCode("""
    function(params) {
        if (params.column.colId === 'EXPENSE_RATIO') {
            const value = parseFloat(params.value);
            if (value > 1.0) {
                return 'High expense ratio - Consider alternatives';
            } else if (value < 0.3) {
                return 'Low expense ratio - Cost efficient';
            }
        }
        if (params.column.colId === 'TICKER_SYMBOL') {
            return `Click for detailed analysis of ${params.value}`;
        }
        return params.value;
    }
    """)

    # Custom column menu items
    custom_menu_items = JsCode("""
    function(params) {
        return [
            'filterMenuTab',
            {
                name: 'Custom Filter',
                action: function() {
                    console.log('Custom filter clicked');
                    params.api.setQuickFilter(params.value);
                }
            },
            {
                name: 'Export Column',
                action: function() {
                    const columnData = [];
                    params.api.forEachNode(node => columnData.push(node.data[params.column.colId]));
                    console.log('Column data:', columnData);
                }
            },
            'separator',
            'columns'
        ];
    }
    """)

    # Row click handler
    row_click_handler = JsCode("""
    function(e) {
        if (e.event.shiftKey) {
            // Handle shift+click
            console.log('Shift clicked row:', e.data);
        } else if (e.event.ctrlKey || e.event.metaKey) {
            // Handle ctrl/cmd+click
            console.log('Ctrl/Cmd clicked row:', e.data);
        } else {
            // Handle normal click
            console.log('Clicked row:', e.data);
        }
    }
    """)

    # Column value formatter
    aum_formatter = JsCode("""
    function(params) {
        if (!params.value) return '';
        const num = parseFloat(params.value.replace(/[^0-9.-]+/g,""));
        if (num >= 1000) {
            return '$' + (num/1000).toFixed(1) + 'B';
        }
        return '$' + num.toFixed(0) + 'M';
    }
    """)

    # Configure default column settings
    gb.configure_default_column(
        editable=True,
        sortable=True,
        filter=True,
        resizable=True,
        cellStyle=cell_style_jscode,
        tooltipComponent=tooltip_jscode,
        menuTabs=['generalMenuTab', 'filterMenuTab', 'columnsMenuTab'],
        wrapHeaderText=True,
        autoHeaderHeight=True,
        enablePivot=True,
        enableValue=True,
        enableRowGroup=True
    )

    # Configure specific columns
    gb.configure_column(
        'TICKER_SYMBOL',
        pinned='left',
        cellRenderer=JsCode("""
        function(params) {
            return `<a href="#" style="text-decoration: none; font-weight: bold; color: #2196F3;">
                ${params.value}
                <span style="font-size: 12px; margin-left: 5px;">📊</span>
            </a>`;
        }
        """)
    )

    gb.configure_column(
        'ASSETS_UNDER_MANAGEMENT',
        valueFormatter=aum_formatter,
        type='numericColumn'
    )

    gb.configure_column(
        'EXPENSE_RATIO',
        type='numericColumn',
        cellEditor='agNumberCellEditor',
        cellEditorParams={'precision': 2}
    )

    # Configure grid features
    gb.configure_grid_options(
        # Row features
        rowSelection='multiple',
        rowMultiSelectWithClick=True,
        rowDragManaged=True,
        onRowClicked=row_click_handler,
        getRowClass=JsCode("""
        function(params) {
            if (params.data && params.data.EXPENSE_RATIO) {
                const value = parseFloat(params.data.EXPENSE_RATIO);
                if (value > 1.5) return 'high-expense-row';
                if (value < 0.2) return 'low-expense-row';
            }
            return '';
        }
        """),
        
        # Selection features
        enableRangeSelection=True,
        enableRangeHandle=True,
        suppressRowClickSelection=False,
        
        # Grouping features
        groupSelectsChildren=True,
        groupSelectsFiltered=True,
        
        # UI features
        enableCharts=True,
        enableRangeHandle=True,
        enableFillHandle=True,
        suppressMovableColumns=False,
        
        # Data features
        enableCellChangeFlash=True,
        enableCellTextSelection=True,
        
        # Export features
        enableCsvExport=True,
        enableExcelExport=True,
        
        # Filtering features
        enableAdvancedFilter=True,
        floatingFilter=True,
        
        # Custom features
        getContextMenuItems=custom_menu_items,
        
        # Status bar configuration
        statusBar={
            'statusPanels': [
                {
                    'statusPanel': 'agTotalAndFilteredRowCountComponent',
                    'align': 'left'
                },
                {
                    'statusPanel': 'agAggregationComponent',
                    'statusPanelParams': {
                        'aggFuncs': ['sum', 'avg', 'min', 'max']
                    }
                },
                {
                    'statusPanel': 'agSelectedRowCountComponent'
                },
                {
                    'statusPanel': 'customStatsPanel',
                    'statusPanelParams': {
                        'template': 
                            '<span class="ag-status-name-value">' +
                            'Selected AUM: <strong>${VALUE}</strong>' +
                            '</span>'
                    }
                }
            ]
        },
        
        # Sidebar configuration
        sideBar={
            'toolPanels': [
                {
                    'id': 'columns',
                    'labelDefault': 'Columns',
                    'labelKey': 'columns',
                    'iconKey': 'columns',
                    'toolPanel': 'agColumnsToolPanel',
                },
                {
                    'id': 'filters',
                    'labelDefault': 'Filters',
                    'labelKey': 'filters',
                    'iconKey': 'filter',
                    'toolPanel': 'agFiltersToolPanel',
                },
                {
                    'id': 'customStats',
                    'labelDefault': 'Statistics',
                    'labelKey': 'statistics',
                    'iconKey': 'chart',
                    'toolPanel': 'customStatsPanel'
                }
            ],
            'position': 'right',
            'defaultToolPanel': 'filters'
        }
    )

    # Custom CSS for grid
    custom_css = {
        ".ag-root-wrapper": {
            "border-radius": "8px",
            "overflow": "hidden",
            "box-shadow": "0 2px 10px rgba(0,0,0,0.1)"
        },
        ".ag-header-cell": {
            "background": "linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%)",
            "font-weight": "600"
        },
        ".ag-row-hover": {
            "background-color": "rgba(33, 150, 243, 0.1) !important"
        },
        ".high-expense-row": {
            "background-color": "rgba(255, 65, 54, 0.05)"
        },
        ".low-expense-row": {
            "background-color": "rgba(46, 204, 64, 0.05)"
        },
        ".ag-status-bar": {
            "font-size": "12px",
            "padding": "8px"
        }
    }

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

@st.dialog("Export Options")
def export_dialog(data):
    st.write("Choose your export file format:")
    export_format = st.radio("Select Export Format", ["CSV", "Excel"])
    
    if st.button("Export"):
        if export_format == "CSV":
            # Generate CSV and download directly
            csv_data = data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Now",
                data=csv_data,
                file_name="exported_data.csv",
                mime="text/csv",
                on_click=lambda: st.session_state.pop("export_dialog", None)
            )
        elif export_format == "Excel":
            # Generate Excel and download directly
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                data.to_excel(writer, index=False, sheet_name='Sheet1')
            output.seek(0)
            excel_data = output.getvalue()
            st.download_button(
                label="Download Now",
                data=excel_data,
                file_name="exported_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                on_click=lambda: st.session_state.pop("export_dialog", None)
            )
def main() -> None:
    st.title("📈 ETF Explorer Pro")
    st.markdown("Explore the Complete list of US ETF's")

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
        final_grid_options, custom_css = configure_grid(filtered_data, group_by_column=None)
        
        col3, col4 = st.columns([1, 9])
        with col3:
            quick_search = st.text_input("Global Quick Search", value="", help="Type to filter all columns globally")
       
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
        with col4:
            if st.button("Export Data"):
                export_dialog(pd.DataFrame(response['data']))
       
        # Trigger modal for export options

  
if __name__ == "__main__":
    main()

    
