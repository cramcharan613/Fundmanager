# Fundmanager

Fundmanager is a Python-based application designed to manage and track financial investments efficiently. This application provides tools to analyze investment performance and generate detailed reports.

## Features

- Track multiple investment portfolios
- Analyze investment performance with visualizations and metrics
- Generate detailed reports in various formats
- Import/export data in CSV and Excel formats

## Installation

To get started with Fundmanager, follow these steps:

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

The main application logic is contained in the `streamlit_app.py` file. Key functionalities include:

- **Data Fetching**: Asynchronously fetch data from external APIs.
- **Data Processing**: Process and enhance data for analysis.
- **UI Rendering**: Render an interactive UI using Streamlit components.

### Key Sections in `streamlit_app.py`

- **Custom CSS**: Defines the styling for the application to enhance the user interface.
- **Data Fetching Class**: `CachedETFDataFetcher` to fetch and cache ETF data.
- **Grid Configuration**: Configure the data grid for displaying ETF data.
- **Export Functionality**: Export data to CSV and Excel formats.

## Contributing

We welcome contributions from the community! Follow these steps to contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Commit your changes and push the branch to your fork.
4. Open a pull request with a description of your changes.

Please ensure your code adheres to the project's coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

## Contact

For any questions or feedback, please open an issue or contact the repository maintainer.
