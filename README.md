
# Fundmanager

Fundmanager is a Python-based application designed to manage and track financial investments efficiently. This application provides tools to analyze investment performance and generate detailed reports.

## Features

- Track multiple investment portfolios
- Analyze investment performance
- Generate detailed reports
- Import/export data in various formats

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/cramcharan613/Fundmanager.git
    cd Fundmanager
    ```

2. **Install the required packages**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Run the application**:
    ```bash
    streamlit run streamlit_app.py
    ```

2. **Open your browser** and navigate to `http://localhost:8501` to access the application.

## Dependencies

The project relies on the following Python packages, as listed in the `requirements.txt` file:
- streamlit
- numpy
- streamlit-aggrid
- pandas
- requests
- urllib3
- openpyxl
- xlsxwriter
- aiohttp
- streamlit_javascript

## Main Python File

The main application logic is contained in the `streamlit_app.py` file. Key functionalities include data fetching, processing, and UI rendering using Streamlit.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## License

This project is licensed under the MIT License.
