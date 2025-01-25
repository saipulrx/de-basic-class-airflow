import requests
from datetime import datetime

# URL of the CSV file
url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"

# Local file name to save the downloaded file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"electric_vehicle_population_data_{timestamp}.csv"

try:
    # Send a GET request to the URL
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check for HTTP request errors

    # Write the content to a local file
    with open(output_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    print(f"File downloaded successfully and saved as '{output_file}'.")

except requests.exceptions.RequestException as e:
    print(f"Failed to download the file: {e}")
