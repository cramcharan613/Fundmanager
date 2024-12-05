import os
from dotenv import load_dotenv
import streamlit as st
from snowflake.snowpark import Session
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import pandas as pd
from functools import lru_cache

load_dotenv()

@st.cache_data(ttl=3600)
def load_data(_session):
    return _session.table("FINANCE.STRAT_PARTNERS.ETF_MAPPING").to_pandas()

@lru_cache(maxsize=1)
def create_snowflake_session():
    load_dotenv()
    conn_params = {
        'account': os.getenv(   'ACCOUNT'),
        'user': os.getenv( 'USER'),
        "AUTHENTICATOR": "externalbrowser",
        'warehouse': os.getenv( 'WAREHOUSE'),
        'database': os.getenv(  'DATABASE'),
        'schema': os.getenv(    'SCHEMA')
    }
    if not all(conn_params.values()):
        st.error("Error: Missing one or more environment variables for Snowflake connection.")
        return None
    return Session.builder.configs(conn_params).create()

def configure_grid(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=True, sortable=True, filter=True, resizable=True)
    gb.configure_selection('multiple', use_checkbox=True)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=100)
    gb.configure_side_bar(filters_panel=True, columns_panel=True)
    button_renderer = JsCode('''
        class ButtonRenderer {
            init(params) {
                this.eGui = document.createElement('button');
                this.eGui.innerHTML = 'View Chart';
                this.eGui.style.cssText = `
                    background-color: #4CAF50;
                    color: white;
                    border: none;
                    padding: 5px 10px;
                    border-radius: 5px;
                    cursor: pointer;
                `;
                this.eGui.addEventListener('click', () => {
                    const modal = document.createElement('div');
                    modal.style.cssText = `
                        position: fixed;
                        top: 0;
                        left: 0;
                        width: 100%;
                        height: 100%;
                        background: rgba(0,0,0,0.5);
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        z-index: 1000;
                    `;
                    const modalContent = document.createElement('div');
                    modalContent.style.cssText = `
                        background: white;
                        padding: 20px;
                        border-radius: 10px;
                        width: 80%;
                        height: 80%;
                        position: relative;
                    `;
                    const closeBtn = document.createElement('button');
                    closeBtn.innerHTML = 'X';
                    closeBtn.style.cssText = `
                        position: absolute;
                        top: 20px;
                        right: 20px;

                        color: red;
                        padding: 1px;
                        border: none;

                        cursor: pointer;
                    `;
                    closeBtn.onclick = function() {
                        document.body.removeChild(modal);
                    };

                    const ticker = params.data.TICKER;
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
                    script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-symbol-overview.js';
                    script.async = true;
                    script.innerHTML = JSON.stringify({
                        "symbols": [
                            [
                                ticker,
                                ticker + "|1D"
                            ]
                        ],
                        "chartOnly": false,
                        "width": "100%",
                        "height": "100%",
                        "locale": "en",
                        "colorTheme": "dark",
                        "autosize": true,
                        "showVolume": false,
                        "showMA": false,
                        "hideDateRanges": false,
                        "hideMarketStatus": false,
                        "hideSymbolLogo": false,
                        "scalePosition": "right",
                        "scaleMode": "Normal",
                        "fontFamily": "-apple-system, BlinkMacSystemFont, Trebuchet MS, Roboto, Ubuntu, sans-serif",
                        "fontSize": "10",
                        "noTimeScale": false,
                        "valuesTracking": "1",
                        "changeMode": "price-and-percent",
                        "chartType": "area",
                        "maLineColor": "#2962FF",
                        "maLineWidth": 1,
                        "maLength": 9,
                        "headerFontSize": "medium",
                        "lineWidth": 2,
                        "lineType": 0,
                        "dateRanges": [
                            "1d|1",
                            "1m|30",
                            "3m|60",
                            "12m|1D",
                            "60m|1W",
                            "all|1M"
                        ]
                    });
                    widgetContainer.appendChild(script);
                });
            }
            getGui() {
                return this.eGui;
            }
        }
    ''')

    gb.configure_column('Action', headerName="Action", cellRenderer=button_renderer)

    gb.configure_grid_options(
        enableRangeSelection=True,
        enableCharts=True,
        suppressRowClickSelection=False,
        enableSorting=True,
        enableFilter=True,
        groupSelectsChildren=True,
        enableColResize=True,
        suppressDragLeaveHidesColumns=True,
        rowSelection='multiple',
        enableStatusBar=True,
        enableFillHandle=True,
        enableRangeHandle=True,
        enableCellChangeFlash=True,
        enableCellTextSelection=True,
        enableClipboard=True,
        enableGroupEdit=True,
        enableCellExpressions=True,
        enableBrowserTooltips=True,
        enableAdvancedFilter=True,
        enableContextMenu=True,
        enableUndoRedo=True,
        enableCsvExport=True,
        enableExcelExport=True,
        enablePivotMode=True,
        enableRtl=True,
        suppressAggFuncInHeader=True,
        suppressColumnVirtualisation=True,
        suppressRowVirtualisation=True,
        suppressMenuHide=True,
        suppressMovableColumns=False,
        suppressFieldDotNotation=True,
        suppressCopyRowsToClipboard=False,
        suppressCopySingleCellRanges=False,
        suppressMultiRangeSelection=False,
        suppressParentsInRowNodes=False,
        suppressTouch=False,
        suppressAsyncEvents=False,
        suppressMakeColumnVisibleAfterUnGroup=False,
        suppressRowGroupHidesColumns=False,
        suppressCellSelection=False,
        suppressColumnMoveAnimation=False,
        suppressAggAtRootLevel=False,
        suppressLoadingOverlay=False,
        suppressNoRowsOverlay=False,
        suppressAutoSize=False,
        animateRows=True,
        allowContextMenuWithControlKey=True,
        suppressContextMenu=False,
        suppressMenuFilterPanel=False,
        suppressMenuMainPanel=False,
        suppressMenuColumnPanel=False
    )
    return gb.build()

def main():
    st.set_page_config(layout="wide")

    session = create_snowflake_session()
    if not session:
        return

    etf = load_data(session)

    st.title("RESEARCH SELECT ETFS")

    grid_options = configure_grid(etf)

    grid_response = AgGrid(
        etf,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode='model_changed',
        data_return_mode='filtered_and_sorted',
        fit_columns_on_grid_load=True,
        height=900,
        allow_unsafe_jscode=True,
        enable_quicksearch=True,
        enable_pagination=True,
        pagination_page_size=100,
        enable_column_resizing=True,
        enable_column_reordering=True,
        enable_column_hiding=True,
        enable_row_hover=True,
        enable_row_dragging=True,
        enable_cell_editing=True,
        enable_cell_selection=True,
        enable_multi_row_selection=True,
        enable_selection_range=True,
        enable_full_screen=True,
        enable_export=True,
        enable_sidebar=True,
        enable_filter_panel=True,
        enable_column_panel=True,
        enable_status_bar=True,
        enable_master_detail=True,
        enable_range_selection=True,
        enable_right_click_selection=True,
        enable_cell_text_selection=True,
        enable_fill_handle=True,
        enable_advanced_filter=True,
        enable_floating_filters=True,
        enable_quick_filter=True,
        enable_infinite_scrolling=True,
        enable_pivot_mode=True,
        enable_undo_redo=True,
        enable_charts=True
    )

    selected_rows = grid_response['selected_rows']
    if len(selected_rows) == 0:
        st.write("No ETFs selected.")
    else:
        st.write("Selected ETFs:")
        st.dataframe(pd.DataFrame(selected_rows))

if __name__ == "__main__":
    main()
