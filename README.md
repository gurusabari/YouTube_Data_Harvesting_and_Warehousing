# YouTube Data Harvesting and Warehousing

## Overview

Welcome to the YouTube Data Harvesting and Warehousing project! This initiative aims to empower users with seamless access to a plethora of YouTube channels' data for analysis. The project employs SQL, MongoDB, and Streamlit to craft a user-friendly application that facilitates the retrieval, storage, and querying of YouTube channel and video data.

## Tools and Libraries Utilized

### Streamlit
The Streamlit library forms the backbone of our user interface, offering an intuitive platform for users to interact with the application and perform various data retrieval and analysis operations.

### Python
Renowned for its ease of learning and understanding, Python serves as the primary programming language for this project. It powers the complete application, handling tasks ranging from data retrieval and processing to analysis and visualization.

### Google API Client
Facilitating communication with different Google APIs, the googleapiclient library in Python plays a crucial role. Specifically, it interacts with YouTube's Data API v3, enabling the retrieval of essential information like channel details, video specifics, and comments. [Learn more](https://developers.google.com/youtube/v3)

### MongoDB
Adopting a scale-out architecture, MongoDB has become a favorite among developers for scalable applications with evolving data schemas. As a document database, MongoDB simplifies the storage of structured or unstructured data using a JSON-like format. [Explore MongoDB](https://www.mongodb.com/)

### PostgreSQL
Known for its reliability and advanced features, PostgreSQL is an open-source and highly scalable database management system. It provides a robust platform for storing and managing structured data, supporting various data types and advanced SQL capabilities. [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## YouTube Data Scraping and Ethical Considerations

When engaging in YouTube content scraping, ethical and responsible practices are paramount. Adherence to YouTube's terms and conditions, obtaining appropriate authorization, and compliance with data protection regulations are fundamental. The collected data must be handled responsibly, ensuring privacy, confidentiality, and prevention of any misuse or misrepresentation. Additionally, considering the potential impact on the platform and its community is crucial for a fair and sustainable scraping process.

## Required Libraries

1. `googleapiclient.discovery`
2. `streamlit`
3. `psycopg2`
4. `pymongo`
5. `pandas`

## Features

The YouTube Data Harvesting and Warehousing application boasts the following functionalities:

- Retrieval of channel and video data from YouTube using the YouTube API.
- Storage of data in a MongoDB database as a data lake.
- Migration of data from the data lake to a SQL database for efficient querying and analysis.
- Search and retrieval of data from the SQL database using different search options.
