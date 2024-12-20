import asyncio
from typing import Dict, Optional
from datetime import datetime
import logging
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode
import pandas as pd
import io
import xlsxwriter
import aiohttp

st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈"
)

logging.basicConfig(level=logging.INFO)
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
                    df[col] = df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else '')
                elif col == 'EXPENSE_RATIO':
                    df[col] = df[col].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else '')
        return df

    async def fetch_stockanalysis_data(self) -> pd.DataFrame:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.stockanalysis_url) as resp:
                    resp.raise_for_status()
                    raw_data = await resp.json()

            if raw_data and 'data' in raw_data:
                raw = raw_data.get("data", {})
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
                if 'CUSIP' in df.columns:
                    cols = ['CUSIP'] + [c for c in df.columns if c != 'CUSIP']
                    df = df[cols]
                return df
            else:
                logger.error("Failed to fetch or process Stock Analysis data")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching Stock Analysis data: {str(e)}")
            return pd.DataFrame()

@st.cache_data()
def load_data():
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
        'pagination': False,  # no pagination for infinite scrolling
        'rowModelType': 'clientSide'
    }

# Custom class representing an ETF object
class ETF:
    def __init__(self, data: Dict[str, str]):
        self.data = data

    def _repr_html_(self):
        # Create a small HTML table to display the ETF data
        html = "<table border='1' style='border-collapse: collapse; font-family: sans-serif; font-size:14px;'>"
        html += "<tr><th colspan='2' style='background:#ddd; padding:8px; text-align:center;'>ETF Details</th></tr>"
        for k, v in self.data.items():
            html += f"<tr><td style='padding:8px; background:#f7f7f7; font-weight:bold;'>{k}</td><td style='padding:8px;'>{v}</td></tr>"
        html += "</table>"
        return html

def main():
    st.title("📈 ETF Explorer Pro")
    st.markdown("""
    Explore ETFs with interactive filtering, grouping, pivoting, and infinite scrolling.
    Use the native Streamlit theme toggle to switch between dark and light modes.
    """)

    with st.spinner("Loading ETF data..."):
        etf_data = load_data()

    if etf_data.empty:
        st.error("No data available.")
        return

    col1, col2 = st.columns([1,9])

    with col1:
        unique_issuers = etf_data['ETF_ISSUER'].dropna().unique()
        selected_issuer = st.selectbox("Filter by ETF Issuer", ["All"] + sorted(unique_issuers.tolist()), index=0)
        group_by_issuer = st.checkbox("Group by ETF Issuer", value=False)

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

    # No filtering done on the Python side except passing full df to grid
    # We'll rely on the grid's filterModel if user selects an issuer
    if selected_issuer == "All":
        filter_model = {}
    else:
        filter_model = {
            "ETF_ISSUER": {
                "filterType": "text",
                "type": "equals",
                "filter": selected_issuer
            }
        }

    with col2:
        quick_search = st.text_input("Global Quick Search", value="", help="Type to filter all columns globally")

        gb = GridOptionsBuilder.from_dataframe(etf_data)
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
        if group_by_issuer and "ETF_ISSUER" in etf_data.columns:
            gb.configure_column("ETF_ISSUER", rowGroup=True, hide=True)

        grid_options = get_grid_options()
        gb.configure_grid_options(**grid_options)

        final_grid_options = gb.build()

        # Apply filter model if issuer is selected
        if filter_model:
            final_grid_options["filterModel"] = filter_model

        if quick_search:
            final_grid_options["quickFilterText"] = quick_search

        response = AgGrid(
            etf_data,
            gridOptions=final_grid_options,
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

# If selected_rows is a list of dictionaries (standard behavior from AgGrid):
        if selected_rows and len(selected_rows) > 0:
            st.subheader("Selected Rows Details")
            etf_objects = [ETF(row) for row in selected_rows]
            for etf_obj in etf_objects:
                st.write(etf_obj)

if __name__ == "__main__":
    main()
