
import asyncio
from typing import Dict, Optional, Any
from datetime import datetime
import logging
import streamlit as st
from st_aggrid import AgGrid, DataReturnMode, GridUpdateMode, JsCode, GridOptionsBuilder
import pandas as pd
import polars as pl
import io
import xlsxwriter
import aiohttp
from functools import lru_cache
import concurrent.futures
from dataclasses import dataclass
from io import StringIO
import boto3
from botocore.exceptions import ClientError
# Load environment variables


st.set_page_config(
    layout="wide",
    page_title="ETF Explorer Pro",
    page_icon="📈"
)



@st.cache_resource
class S3Service:
    def __init__(self):
        self.s3_client = None
        self.bucket = "cetera-finance-1"
        self.prefix = "daily_etf_ts"
        
    @st.dialog("S3 Configuration and Log")
    def configure_s3(self):
        """Dialog for S3 configuration and logging"""
        with st.container():
            st.subheader("AWS S3 Configuration")
            
            # Create columns for better layout
            col1, col2 = st.columns(2)
            
            with col1:
                aws_access_key = st.text_input(
                    "AWS Access Key ID",
                    type="password",
                    help="Enter your AWS Access Key ID"
                )
                
                aws_secret_key = st.text_input(
                    "AWS Secret Access Key",
                    type="password",
                    help="Enter your AWS Secret Access Key"
                )
                
                aws_region = st.text_input(
                    "AWS Region",
                    value="us-west-2",
                    help="Enter AWS Region (default: us-west-2)"
                )
            
            with col2:
                st.markdown("### Connection Status")
                status_placeholder = st.empty()
                
                # Initialize connection button
                if st.button("Connect to S3"):
                    try:
                        self.s3_client = boto3.client(
                            service_name='s3',
                            aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_secret_key,
                            region_name=aws_region,
                            verify=False
                        )
                        self._ensure_bucket_exists()
                        status_placeholder.success("✅ Successfully connected to S3")
                    except Exception as e:
                        status_placeholder.error(f"❌ Connection failed: {str(e)}")
                        self.s3_client = None
            
            # Add logging section
            st.markdown("### S3 Operations Log")
            log_container = st.container()
            
            # Create expander for bucket operations
            with st.expander("Bucket Operations", expanded=False):
                if st.button("List Buckets"):
                    try:
                        if self.s3_client:
                            response = self.s3_client.list_buckets()
                            buckets = [bucket['Name'] for bucket in response['Buckets']]
                            st.write("Available buckets:", buckets)
                        else:
                            st.warning("No S3 connection available")
                    except Exception as e:
                        st.error(f"Failed to list buckets: {str(e)}")
                
                if st.button("Check Current Bucket"):
                    self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Check if bucket exists and create if it doesn't"""
        try:
            if self.s3_client is None:
                st.warning("No S3 connection available")
                return

            self.s3_client.head_bucket(Bucket=self.bucket)
            st.success(f"✅ Bucket {self.bucket} exists")
            logger.info(f"Bucket {self.bucket} exists")
            
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                try:
                    st.info(f"Creating bucket {self.bucket}...")
                    if self.s3_client.meta.region_name == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={
                                'LocationConstraint': 'us-west-2'
                            }
                        )
                    st.success(f"✅ Successfully created bucket {self.bucket}")
                    logger.info(f"Successfully created bucket {self.bucket}")
                except Exception as create_error:
                    st.error(f"Failed to create bucket: {str(create_error)}")
                    logger.error(f"Failed to create bucket: {str(create_error)}")
            else:
                st.error(f"Error checking bucket: {str(e)}")
                logger.error(f"Error checking bucket: {str(e)}")

    def auto_save_to_s3(self, df: pl.DataFrame) -> None:
        """Automatically save DataFrame to S3"""
        try:
            if self.s3_client is None:
                st.warning("No S3 connection available")
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            key = f"{self.prefix}/etf_data_{timestamp}.csv"

            with st.spinner("Saving to S3..."):
                csv_buffer = StringIO()
                df.write_csv(csv_buffer)

                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )

                st.success(f"✅ Successfully saved data to S3: s3://{self.bucket}/{key}")
                logger.info(f"Successfully saved data to S3: s3://{self.bucket}/{key}")

        except Exception as e:
            st.error(f"Error saving to S3: {str(e)}")
            logger.error(f"Error saving to S3: {str(e)}")
            raise


# Custom CSS#-------------------------------------------------------------------------------------------
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



/* Multi-select fixes */
.stApp .stMultiSelect > div {
  pointer-events: auto !important;
}
</style>
""", unsafe_allow_html=True)
#-------------------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#-------------------------------------------------------------------------------------------
cdo_logo = """ 

<style>
@keyframes fadeInSlideIn {
    0% { opacity: 0; transform: translateY(-20px); }
    100% { opacity: 1; transform: translateY(0); }
}
.logo-container {
    display: flex;
    align-items: center;
    margin-bottom: 5px;
    animation: fadeInSlideIn 1s ease-out;
}
.logo { animation: fadeInSlideIn 1s ease-out 0.5s both; }
.title {
    margin-left: 20px;
    color: #7c8096;
    text-shadow: 2px 2px 4px #272053;
    animation: fadeInSlideIn 1s ease-out 1s both;
}
.sub-header {{
    margin-top: 5px;
    color: #5a5a5a;
    font-size: 1.2em;
    animation: fadeInSlideIn 1s ease-out 1.5s both;
}}


.stApp .stMultiSelect > div {
    pointer-events: auto !important;
}
</style>

<div class="logo-container">
     <svg fill="none" height="136"  viewbox="0 0 115 136" width="136"
     xmlns="http://www.w3.org/2000/svg">
    <g filter="url(#filter0_d_1_6)">
        <path class="animate" d="M107.447 0.0193964L103.207 17.5622L90.7955 4.37679L107.447 0.0193964Z" fill="#339966"/>
        <path class="animate" d="M107.447 0.0193964L103.207 17.5622L90.7955 4.37679L107.447 0.0193964Z" fill="#339966" stroke="none"/>
        <path class="animate" d="M93.7094 6.94159C93.9007 6.7385 94.2126 6.74028 94.4021 6.94553L100.781 13.8562C100.856 13.9367 100.786 14.0698 100.682 14.0463C100.646 14.0382 100.609 14.0498 100.583 14.0771L85.0963 30.4657L69.1845 47.3899L31.8587 87.3438C31.8417 87.3619 31.8261 87.3814 31.8119 87.4019L29.1551 91.2535L28.3035 92.9341L27.3306 94.7437L25.5074 98.8781L24.037 103.088C23.9712 103.276 24.0157 103.488 24.1509 103.629L26.4057 105.989C26.5367 106.127 26.5829 106.33 26.525 106.515L26.2555 107.375L25.8924 109.052L25.6534 111.759L25.5383 115.495L25.2988 117.944L18.8469 111.832C18.8275 111.814 18.8095 111.794 18.7931 111.773L13.5981 104.972C13.4954 104.837 13.4637 104.658 13.5137 104.493L16.1026 95.9792L18.0477 91.5861L20.115 87.4502L22.3057 83.9574L24.7407 80.5918L29.4911 75.1496L34.4089 69.9639L38.0187 66.0752L46.6681 56.8711L93.7094 6.94159Z" fill="#339966"/>
        <path class="animate" opacity="0.85" fill-rule="evenodd" clip-rule="evenodd" d="M51.3861 125.217L51.3552 123.808L50.8944 125.158C51.0582 125.179 51.222 125.198 51.3861 125.217ZM14.9879 80.9875C15.4781 83.4609 16.182 85.8718 17.0852 88.1959L16.9472 88.1494L12.5578 101.015C9.05764 95.5243 6.57927 89.3964 5.29548 82.9184C2.90937 70.8782 4.79766 58.3736 10.6386 47.5352C16.4796 36.6969 25.9118 28.1954 37.3281 23.4794C48.744 18.7635 61.4376 18.1246 73.2462 21.6713L65.706 29.5064C62.8708 29.0016 59.9428 28.77 56.9512 28.8394C34.5722 29.3587 16.4894 46.5391 14.3972 68.1977C13.9579 72.4382 14.1478 76.7482 14.9879 80.9875ZM70.3868 30.6097L70.4958 30.642L70.5613 30.4285L70.3868 30.6097Z" fill="#339966"/>
        <path class="animate" d="M87.3062 35.3123C87.51 35.0933 87.8495 35.1106 88.0327 35.3493L88.548 36.0205L89.6528 37.1359L90.5119 38.2551L91.3712 39.1154L93.3353 41.0911L94.1966 41.9405C94.3971 42.1383 94.4033 42.4714 94.2102 42.677L65.9384 72.7789L50.5818 89.3286L46.6504 93.4027L45.2683 95.0423L42.6271 98.3462C42.6065 98.3719 42.5884 98.3997 42.573 98.4291L40.7526 101.909L39.7695 103.801L39.032 105.69L38.4172 107.706L38.0481 109.718L37.4574 112.461C37.4413 112.536 37.4413 112.614 37.4574 112.689L37.6287 113.485C37.6626 113.643 37.765 113.774 37.905 113.84L39.2743 114.487L40.4079 114.868C40.6103 114.936 40.7477 115.134 40.7476 115.358L40.7467 117.83L40.9934 120.803L41.6086 123.77L41.9557 125.108C41.9694 125.161 41.9918 125.211 42.0217 125.256C42.2732 125.632 41.9178 126.126 41.5067 125.972L39.5204 125.227L35.4684 123.607L31.0484 121.347L27.6064 119.223C27.457 119.131 27.3651 118.962 27.365 118.78L27.3646 117.392L27.4879 114.736L27.6109 112.599L27.9818 109.033L28.5979 106.241L29.4587 103.25L30.1941 100.909L31.1752 98.5636L33.5101 94.1262L35.1689 91.5764L37.319 89.0189L43.8304 81.7974L51.3245 73.5255L59.3099 65.1164L73.8678 49.7442L87.3062 35.3123Z" fill="#46286E"/>
        <path class="animate" d="M101.717 28.1221L96.8848 45.0532L84.3288 33.042L101.077 28.238L101.717 28.1221Z" fill="#46286E"/>
        <path class="animate" d="M84.2943 63.7657C86.3088 61.5443 89.7024 61.7564 91.46 64.2137C92.9266 66.2641 92.7117 69.1647 90.9616 70.9439L83.6621 78.3641L69.2484 93.5159C68.7039 94.0883 67.7823 93.6876 67.7672 92.872C67.7632 92.6538 67.6874 92.444 67.553 92.2795L66.5364 91.0346C64.8159 88.9275 64.859 85.7748 66.6364 83.722L75.9216 72.9979L84.2943 63.7657Z" fill="#5D6972"/>
        <path class="animate" d="M92.9372 73.9895L80.8439 62.2679L96.9971 55.9456L96.6542 57.8749L92.9372 73.9895Z" fill="#5D6972"/>
        <path class="animate" d="M107.754 85.6843C111.702 73.5977 110.854 59.7016 106.959 48.622L99.5326 54.1993C101.605 60.1396 102.375 66.8414 101.538 73.9274C98.6819 98.1221 79.4779 117.375 55.4842 117.661C53.9857 117.654 52.5163 117.56 51.0796 117.384L46.7663 126.804C48.779 127.14 50.834 127.34 52.9205 127.4C64.4302 127.726 76.1859 123.747 86.1844 116.141C96.1829 108.534 103.806 97.7708 107.754 85.6843Z" fill="#5D6972"/>
        <path class="animate" d="M82.2477 65.9865C82.4566 65.7695 82.801 65.7853 82.9922 66.0208L83.4877 66.6307L84.5397 67.7161L85.391 68.764L86.2083 69.6024L88.0776 71.5262L88.8763 72.337C89.0779 72.5416 89.0795 72.8768 88.88 73.0818L69.0011 93.5109L59.407 103.508L56.5165 106.36L56.0866 106.865L55.943 107.006L54.718 108.382L53.6956 109.919C53.3277 110.379 53.0203 110.928 52.7913 111.535C52.5368 112.21 52.3851 112.942 52.3468 113.678C52.3086 114.414 52.3846 115.138 52.5697 115.798C52.7547 116.459 53.0443 117.04 53.4182 117.502L53.5073 117.362L53.8292 117.837C53.8734 117.902 53.9313 117.956 53.9985 117.995L54.4272 118.241C54.5005 118.283 54.5825 118.306 54.6662 118.307L60.1001 118.385C60.132 118.386 60.1638 118.383 60.1952 118.377L62.6798 117.921L65.5589 117.701L66.401 117.548L67.2222 117.463C67.4467 117.44 67.6594 117.574 67.7434 117.791L68.2323 119.056C68.26 119.127 68.3026 119.192 68.3571 119.244L69.634 120.48C69.6644 120.509 69.6912 120.542 69.7138 120.578L70.1302 121.245C70.3102 121.533 70.179 121.919 69.8648 122.025L57.8872 126.076L54.9871 126.754L52.2857 127.083C51.9661 127.122 51.6428 127.118 51.3237 127.073L50.1596 126.906C50.061 126.891 49.9878 126.803 49.9891 126.7C49.9906 126.577 49.8867 126.48 49.7683 126.494L47.0673 126.826C47.0077 126.834 46.9474 126.83 46.889 126.815L46.7721 126.786L46.2337 126.641C46.2018 126.632 46.1707 126.62 46.1409 126.605L45.5114 126.289C45.4129 126.24 45.3324 126.159 45.282 126.058L44.3428 124.184C44.3274 124.154 44.3149 124.121 44.3057 124.088L43.6635 121.785L43.1678 119.734L43.0354 118.519L43.0022 117.646L42.9818 115.764L43.0832 113.402L43.3725 111.247L43.7447 109.468L44.2888 107.607C44.2965 107.58 44.3062 107.555 44.3178 107.53L45.6216 104.751L46.6447 102.961L47.5518 101.727L48.5925 100.271L50.2677 98.4716L52.4557 96.2819L57.6934 90.8374L60.5306 88.0942L63.1275 85.3501L73.2691 75.3135L82.2477 65.9865Z" fill="#5D6972"/>
    </g>
        <defs>
            <filter id="filter0_d_1_6" x="0.289368" y="0.0194092" width="114.04" height="135.399" filterunits="userSpaceOnUse" color-interpolation-filters="sRGB">
                <feflood flood-opacity="0" result="BackgroundImageFix"/>
                <fecolormatrix in="SourceAlpha" type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 127 0" result="hardAlpha"/>
                <feoffset dy="4"/>
                <fegaussianblur stdeviation="2"/>
                <fecomposite in2="hardAlpha" operator="out"/>
                <fecolormatrix type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0.25 0"/>
                <feblend mode="normal" in2="BackgroundImageFix" result="effect1_dropShadow_1_6"/>
                <feblend mode="normal" in="SourceGraphic" in2="effect1_dropShadow_1_6" result="shape"/>
            </filter>
        </defs>
    </svg>
       <h1 class="title">ETF EXPLORER</h1>
        <h2 class="sub-header">Explore the Complete list of US ETF's</h2>
</div>


"""
st.markdown(cdo_logo,unsafe_allow_html=True)


def _set_block_container_style(
    max_width: int = 1200,
    max_width_100_percent: bool = True,
    padding_top: int = 0,
    padding_right: int = 0,
    padding_left: int = 0,
    padding_bottom: int = 0,
    ):
    if max_width_100_percent:
        max_width_str = f"max-width: 100%;"
    else:
        max_width_str = f"max-width: {max_width}px;"
        
    styl = f"""
    <style>
        .reportview-container .main .block-container{{
            {max_width_str}
            padding-top: {padding_top}rem;
            padding-right: {padding_right}rem;
            padding-left: {padding_left}rem;
            padding-bottom: {padding_bottom}rem;
        }}
        }}
    </style>
    """
    st.markdown(styl, unsafe_allow_html=True)



def _set_st_app_style(
    header_bg_color: str = "transparent",
    select_div_bg_color: str = "transparent",
    select_div_width: str = "100%",
    data_frame_bg_color: str = "transparent",
    data_frame_width: str = "100%",
    data_frame_border: str = "1px solid #0f1532",
    data_frame_border_radius: str = "4px",
    data_frame_padding: str = "2px",
    data_frame_margin: str = "10px",
    data_frame_font_size: str = "10px",
    data_frame_font_color: str = "#333",
    font_family: str = "-apple-system, BlinkMacSystemFont, sans-serif",
    gradient_colors: str = "#4c234a, #0f1532",
    height: str = "100vh",
    width: str = "100vw",
    box_shadow: str = "0 4px 8px rgba(0, 0, 0, 0.2)",
    transition: str = "all 0.3s ease",
    text_color: str = "#ffffff",
    radial_gradient_color: str = "rgba(255,255,255,0.1)",
    radial_gradient_size_1: str = "50px 50px",
    radial_gradient_size_2: str = "100px 100px",
    star_animation_duration_1: str = "30s",
    star_animation_duration_2: str = "60s",
    opacity_1: float = 0.5,
    opacity_2: float = 0.3,
    padding_top: int = 0,
    padding_right: int = 0,
    padding_left: int = 0,
    padding_bottom: int = 0,
    ):
    styl = f"""
    <style>
        /* Header Styling */
        .stApp > header {{
            background-color: {header_bg_color};
        }}
        

        
        /* Select Box Styling */
        div[data-baseweb="select"] > div {{
            background-color: {select_div_bg_color};
            width: {select_div_width};
            padding-top: {padding_top}rem;
            padding-right: {padding_right}rem;
            padding-left: {padding_left}rem;
            padding-bottom: {padding_bottom}rem;
        }}
        
        /* DataFrame Styling */
        div[data-testid="stDataFrame"] {{
            background-color: {data_frame_bg_color};
            width: {data_frame_width};
            border: {data_frame_border};
            border-radius: {data_frame_border_radius};
            padding: {data_frame_padding};
            margin: {data_frame_margin};
            font-size: {data_frame_font_size};
            color: {data_frame_font_color};
        }}
        
        /* Main App Container Styling */
        .stApp {{
            position: relative; /* Changed from margin: fixed; */
            font-family: {font_family};
            overflow: hidden; /* Changed from overflow: None; to a valid value */
            background: linear-gradient(to top, {gradient_colors});
            animation: gradientAnimation 15s ease infinite;
            height: {height};
            width: {width};
            background-attachment: fixed;
            box-shadow: {box_shadow};
            transition: {transition};
            color: {text_color}; /* Ensuring text is readable */
        }}

        /* Pseudo-elements for Star Animation */
        .stApp::before, .stApp::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            background: transparent;
            z-index: -1; /* Ensure stars are behind content */
        }}

        .stApp::before {{
            background: radial-gradient(circle, {radial_gradient_color} 1px, transparent 1px);
            background-size: {radial_gradient_size_1};
            animation: starsAnimation1 {star_animation_duration_1} linear infinite;
            opacity: {opacity_1};
        }}

        .stApp::after {{
            background: radial-gradient(circle, {radial_gradient_color} 2px, transparent 2px);
            background-size: {radial_gradient_size_2};
            animation: starsAnimation2 {star_animation_duration_2} linear infinite;
            opacity: {opacity_2};
        }}

        /* Keyframes for Star Animations */
        @keyframes starsAnimation1 {{
            from {{
                transform: translateY(0);
            }}
            to {{
                transform: translateY(-200%);
            }}
        }}

        @keyframes starsAnimation2 {{
            from {{
                transform: translateY(0);
            }}
            to {{
                transform: translateY(-200%);
            }}
        }}

        /* Gradient Animation */
        @keyframes gradientAnimation {{
            0% {{
                background-position: 0% 50%;
            }}
            50% {{
                background-position: 100% 50%;
            }}
            100% {{
                background-position: 0% 50%;
            }}
        }}
        
        /* Subheader Styling */
        .sub-header {{
            margin-top: 5px;
            color: #5a5a5a;
            font-size: 1.2em;
            animation: fadeInSlideIn 1s ease-out 1.5s both;
        }}
    </style>
    """
    st.markdown(styl, unsafe_allow_html=True)

# Call the styling function with default or customized parameters
_set_st_app_style()

_set_st_app_style()
_set_block_container_style()

#-------------------------------------------------------------------------------------------
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
def load_data() -> pl.DataFrame:
    fetcher = CachedETFDataFetcher()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Convert pandas DataFrame to Polars
    pandas_df = loop.run_until_complete(fetcher.fetch_data())
    return pl.from_pandas(pandas_df)

def save_to_s3(df: pl.DataFrame, bucket: str, key: str):
    """Save DataFrame to S3 bucket"""
    try:
        s3_client = boto3.client('s3')
        csv_buffer = StringIO()
        df.write_csv(csv_buffer)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        return True
    except Exception as e:
        st.error(f"Error saving to S3: {str(e)}")
        return False

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
        sizeColumnsToFit=True,
        enableRowGroup=True,
        enablePivot=True,
        enableValue=True,
        wrapHeaderText=True,
        autoHeaderLabel=True,
        autoHeaderTooltip=True,
        autoHeaderCellFilter=True,
        autoHeaderCellRenderer=True,
        autoHeaderHeight=True,
        autoSizeColumns=True,
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
    try:
        # Initialize S3 Service
        s3_service = S3Service()
        
        if st.sidebar.button("Configure S3"):
            s3_service.configure_s3()
        
       
  
        with st.spinner("Loading ETF data..."):
            etf_data = load_data()
              
            # Continue with the rest of your application...
      
            if etf_data.height == 0:  # Correct way to check if Polars DataFrame is empty
                st.error("No data available.")
                return

        col1, col2 = st.columns([1, 9])

        with col1:
            # Convert to pandas for these operations or use Polars equivalent
            etf_data_pd = etf_data.to_pandas()
            
            issuers = sorted(etf_data_pd['ETF_ISSUER'].dropna().unique().tolist())
            selected_issuer = st.selectbox("Filter by ETF Issuer", ["All"] + issuers, index=0)

            asset_classes = sorted(etf_data_pd['ASSET_CLASS'].dropna().unique().tolist())
            selected_asset_class = st.selectbox("Filter by Asset Class", ["All"] + asset_classes, index=0)

            # Clean and convert AUM values
            numeric_aum = (etf_data_pd['ASSETS_UNDER_MANAGEMENT']
                         .str.replace('$', '', regex=False)
                         .str.replace(',', '', regex=False)
                         .str.replace('M', '', regex=False))
            numeric_aum = pd.to_numeric(numeric_aum, errors='coerce')
            max_aum = int(numeric_aum.max()) if numeric_aum.notnull().any() else 0
            min_aum = st.slider("Min AUM ($M)", min_value=0, max_value=max_aum, value=0)

        # Filter data
        filtered_data = etf_data_pd.copy()
        if selected_issuer != "All":
            filtered_data = filtered_data[filtered_data['ETF_ISSUER'] == selected_issuer]
        if selected_asset_class != "All":
            filtered_data = filtered_data[filtered_data['ASSET_CLASS'] == selected_asset_class]

        numeric_filtered_aum = (filtered_data['ASSETS_UNDER_MANAGEMENT']
                              .str.replace('$', '', regex=False)
                              .str.replace(',', '', regex=False)
                              .str.replace('M', '', regex=False))
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

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error(f"An error occurred: {str(e)}")
        
if __name__ == "__main__":
    main()
