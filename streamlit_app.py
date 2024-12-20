# [Previous imports remain the same...]

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
                "header_screenshot",
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
            # Add custom metrics
            st.markdown("""
            <div class="stats-card">
                <h4>Trading Signals</h4>
                <div id="trading-signals"></div>
            </div>
            """, unsafe_allow_html=True)
            
    with tab3:
        st.components.v1.html(
            create_tradingview_company_profile(ticker),
            height=450
        )

def main() -> None:
    # [Previous main code remains the same until the grid configuration]

    # Modify the grid configuration to include the enhanced chart button
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

    # Add JavaScript handler for chart display
    st.markdown("""
    <script>
    function showAdvancedChart(ticker) {
        // Create modal for chart display
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
        
        // Add close button
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
        
        // Initialize TradingView widget in modal
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

    # [Rest of the main code remains the same]

if __name__ == "__main__":
    main()
