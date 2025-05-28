# Shamela Text Extraction Pipeline

A complete pipeline for extracting text content from Shamela (المكتبة الشاملة) digital Islamic library into structured JSON files.

## Overview

This tool extracts content from Shamela's Lucene indices and SQLite databases, converting them into clean JSON files with complete metadata including:
- Full book text with page structure
- Author information and metadata
- Category classifications
- Title hierarchies
- Footnotes and references
- Requires ~40GB of disk space for extraction of full Shamela installation

## Prerequisites

### System Requirements
- **Java 8+** (required for Lucene processing)
- **Python 3.7+** with the following packages:
  - `pandas`
  - `sqlite3` (usually included with Python)
  - `pathlib` (usually included with Python)

### Required Files
1. **Shamela Installation** - Complete Shamela4 directory with the following (included in full shamela instalation):
   - `database/store/` (Lucene indices)
   - `database/master.db` (main metadata database)  
   - `database/book/` (individual book databases)

2. **Java Dependencies** - Already included in this repository:
   - `lib/lucene-core-9.10.0.jar`
   - `lib/lucene-backward-codecs-9.10.0.jar`

## Installation

1. **Clone this repository:**
   ```bash
   git clone https://github.com/ammusto/shamela-extractor
   cd shamela-extractor
   ```

2. **Install Python dependencies:**
   ```bash
   pip install pandas
   ```

3. **Verify Java installation:**
   ```bash
   java -version
   ```

4. **Compile the java extractor:**
    ```bash
    javac -cp "lib/*" java/ShamelaIndexExporter.java
    ```

## Directory Structure

Your setup should look like this:
```
shamela-extractor/
├── lib/
│   ├── lucene-core-9.10.0.jar
│   └── lucene-backward-codecs-9.10.0.jar
├── java/
│   ├── ShamelaIndexExporter.java
│   ├── ShamelaIndexExporter.class (after compilation)
│   ├── ShamelaIndexExporter$TitleDoc.class (after compilation)
│   └── ShamelaIndexExporter$DocWithId.class (after compilation)
├── extract_indices.py
├── build_jsons.py
└── README.md

Default shamela installation:

.../shamela4/
├── database/
│   ├── store/           (Lucene indices)
│   ├── master.db        (main database)
│   └── book/            (individual book databases)
└── ...
```

## Usage

### Step 1: Extract Lucene Indices

```bash
python extract_indices.py
```

**What it does:**
- Extracts all Lucene indices to CSV files
- Creates individual CSV files for each book's content (~17GB for full Shamela installation)
- Exports author and book metadata
- Validates prerequisites automatically

**You'll be prompted for:**
- Shamela base path (e.g., `C:/shamela4` or `/path/to/shamela4`)
- Output directory (default: current directory)

**Output:**
- `exported_indices/book_data/` - Individual book CSV files (`1000.csv`, `1001.csv`, etc.)
- `exported_indices/author.csv` - Author metadata
- `exported_indices/book.csv` - Book metadata
- `logs/lucene_extraction.log` - Processing log

### Step 2: Build JSON Files

```bash
python build_jsons.py
```

**What it does:**
- Combines CSV data with SQLite metadata
- Creates structured JSON files for each book
- Processes text content and hierarchies
- Links authors, categories, and metadata

**You'll be prompted for:**
- Shamela base path (same as Step 1)
- Extracted data path (where Step 1 outputs are located, defaults to cwd)

**Output:**
- `books_json/1000.json`, `books_json/1001.json`, etc. - Complete book JSON files
- `logs/json_building.log` - Processing log


### Step 3 (optional): Clean CSV Files after building JSONs

```bash
python clean_files.py
```

**What it does:**
- Deletes book_data folder and all book-level CSV files (~17GB) (no longer needed)

## Output Format

Each book JSON contains:
```json
{
    "book_id": "1",
    "title": "الفواكه العذاب في الرد على من لم يحكم السنة والكتاب",
    "book_date": 1225,
    "category": {
        "id": 1,
        "name": "العقيدة"
    },
    "book_type": 1,
    "printed": 1,
    "pdf_links": {
        "files": [
            "/1/41557.pdf"
        ],
        "cover": 2,
        "size": 1445468
    },
    "meta_data": {
        "date": "08121431"
    },
    "authors": [
        {
            "id": "513",
            "name": "حمد بن ناصر آل معمر",
            "death_number": 1225,
            "death_text": "1225",
            "is_main_author": true
        }
    ],
    "book_meta": [
        "الكتاب: الفواكه العذاب في الرد على من لم يحكّم السنة والكتاب",
        "..."
    ],
    "author_meta": [
        "حمد بن ناصر آل معمر (٠٠٠ - ١٢٢٥ هـ = ٠٠٠ - ١٨٣٩ م)",
        "..."
    ],
    "parts": [
        {
            "part": "",
            "pages": [
                {
                    "page_id": "1",
                    "page_number": "3",
                    "body": "...",
                    "footnote": "..."
                },
              ...
            ]
        }
    ]
}
```

## Monitoring Progress

**Log Files:**
- Complete processing logs in `logs/` directory
- Every book processing result logged
- Error details for troubleshooting

## Troubleshooting

**"Java is not installed or not in PATH"**
- Install Java JDK/JRE 8 or higher
- Ensure `java` command works in terminal

**"No book content CSV files found!"**
- Run `extract_indices.py` first
- Check that `exported_indices/book_data/` contains CSV files

**"Shamela Lucene store not found"**
- Verify Shamela installation path

**Books fail to process**
- Check `logs/json_building.log` for specific errors
- Verify individual book databases in `database/book/` exist

## Output Size Expectations

For a complete Shamela library:
- **CSV files:** ~17GB
- **JSON files:** ~18GB
- **Processing time:** 30 minutes+ for extracting indices and 30+ minutes for building JSONs

## File Naming Convention

- **Input CSV files:** `1000.csv`, `1001.csv`, etc. (book IDs)
- **Output JSON files:** `1000.json`, `1001.json`, etc. (matching book IDs)
- **Log files:** Timestamped in `logs/` directory

## License

MIT License

## Support

For issues or questions:
1. Check the log files in `logs/` directory
2. Verify your Shamela installation is complete
3. Ensure all prerequisites are installed
