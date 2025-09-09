import os
import sqlite3
import json
import glob
from collections import defaultdict
from datetime import datetime
import re
import pandas as pd
from pathlib import Path
import sys
import argparse

class JSONBuilder:
    def __init__(self, shamela_base_path, extracted_data_path, test_mode=False, generate_toc=False):
        """
        Initialize the json builder
        
        Args:
            shamela_base_path: Path to shamela4 directory
            extracted_data_path: Path where CSV files were extracted
            test_mode: If True, process only the first book found
            generate_toc: If True, generate table of contents and remove HTML tags
        """
        self.test_mode = test_mode
        self.generate_toc = generate_toc
        self.shamela_base_path = Path(shamela_base_path)
        self.extracted_data_path = Path(extracted_data_path)
        
        # Key paths
        self.master_db_path = self.shamela_base_path / "database" / "master.db"
        self.books_db_path = self.shamela_base_path / "database" / "book"
        self.csv_input_path = self.extracted_data_path / "exported_indices"
        
        # Output paths
        self.json_output_dir = self.extracted_data_path / "books_json"
        self.log_dir = self.extracted_data_path / "logs"
        
        # Create directories
        self.json_output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)
    
    def check_prerequisites(self):
        """Check if required input files exist"""
        print("Checking prerequisites...")
        
        issues = []
        
        # Check Shamela master database
        if not self.master_db_path.exists():
            issues.append(f"Shamela master.db not found: {self.master_db_path}")
        else:
            print(f"  [OK] Master database found: {self.master_db_path}")
        
        # Check book databases directory
        if not self.books_db_path.exists():
            issues.append(f"Book databases directory not found: {self.books_db_path}")
        else:
            book_count = len(list(self.books_db_path.glob("**/*.db")))
            print(f"  [OK] Book databases found: {book_count:,} files")
        
        # Check CSV input directory
        if not self.csv_input_path.exists():
            issues.append(f"CSV input directory not found: {self.csv_input_path}")
            issues.append("  → Run extract_indices.py first!")
        else:
            print(f"  ✓ CSV input directory found: {self.csv_input_path}")
        
        # Check key CSV files
        required_files = [
            self.csv_input_path / "author.csv",
            self.csv_input_path / "book.csv",
            self.csv_input_path / "book_data"
        ]
        
        for required_file in required_files:
            if not required_file.exists():
                issues.append(f"Required file/directory not found: {required_file.name}")
            else:
                if required_file.is_file():
                    size = required_file.stat().st_size / (1024**2)
                    print(f"  ✓ {required_file.name} ({size:.1f} MB)")
                else:
                    count = len(list(required_file.glob("*.csv")))
                    print(f"  ✓ {required_file.name}/ ({count:,} CSV files)")
        
        if issues:
            print("\n❌ Issues found:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print("  ✓ All prerequisites satisfied!")
        return True
    
    def build_json_files(self):
        """Build individual JSON files for each book"""
        print("="*60)
        print("BUILDING JSON FILES")
        print("="*60)
        
        if not self.check_prerequisites():
            return False
        
        # Load metadata
        print("Loading metadata...")
        metadata = self._load_metadata()
        
        # Get list of book content files
        book_data_dir = self.csv_input_path / "book_data"
        book_csv_files = list(book_data_dir.glob("*.csv"))
        
        print(f"Found {len(book_csv_files):,} book content files")
        
        if not book_csv_files:
            print("❌ No book content CSV files found!")
            return False
        
        # Process books
        log_file = self.log_dir / "json_building.log"
        self._log(log_file, "Starting JSON building process")
        self._log(log_file, f"Processing {len(book_csv_files):,} books")
        
        processed_count = 0
        failed_count = 0
        start_time = datetime.now()
        
        for i, csv_file in enumerate(book_csv_files):
            # Extract book ID from filename (book_1000.csv -> 1000)
            book_id = csv_file.stem
            
            try:
                book_data = self._process_single_book(book_id, metadata)
                
                if book_data:
                    # Write as formatted JSON
                    json_path = self.json_output_dir / f"{book_id}.json"
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(book_data, f, ensure_ascii=False, indent=2)
                    
                    processed_count += 1
                    self._log(log_file, f"SUCCESS: Book {book_id} -> {json_path.name}")
                else:
                    failed_count += 1
                    self._log(log_file, f"FAILED: Book {book_id} - No data generated")
                
            except Exception as e:
                failed_count += 1
                self._log(log_file, f"ERROR: Book {book_id} - {str(e)}")
            
            # Progress update every 100 books (or always in test mode)
            if (i + 1) % 100 == 0 or self.test_mode:
                elapsed = datetime.now() - start_time
                rate = (i + 1) / elapsed.total_seconds() * 60  # books per minute
                print(f"  Progress: {i + 1:,}/{len(book_csv_files):,} "
                      f"({processed_count:,} successful, {failed_count:,} failed) "
                      f"[{rate:.1f} books/min]")
            
            # Exit after first book in test mode (regardless of success/failure)
            if self.test_mode:
                print(f"\nTEST MODE: Stopping after first book attempt (ID: {book_id})")
                self._log(log_file, f"TEST MODE: Stopped after processing book {book_id}")
                break
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\nJSON building completed!")
        print(f"Duration: {duration}")
        print(f"Processed: {processed_count:,} books successfully")
        print(f"Failed: {failed_count:,} books")
        
        if processed_count > 0:
            self._show_json_summary()
        
        self._log(log_file, f"JSON building completed: {processed_count} success, {failed_count} failed")
        self._log(log_file, f"Total duration: {duration}")
        return processed_count > 0
    
    def _load_metadata(self):
        """Load all metadata from CSV files (extracts from master.db if needed)"""
        metadata_dir = self.extracted_data_path / "exported_metadata"
        
        # Extract master.db tables if metadata directory doesn't exist
        if not metadata_dir.exists() or not list(metadata_dir.glob("*.csv")):
            print("Extracting all tables from master.db...")
            metadata_dir.mkdir(exist_ok=True)
            
            conn = sqlite3.connect(self.master_db_path)
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"Found {len(tables)} tables in master.db")
            
            for (table_name,) in tables:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                    csv_path = metadata_dir / f"{table_name}.csv"
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f"  Exported {table_name}: {len(df)} rows")
                except Exception as e:
                    print(f"  Error exporting {table_name}: {e}")
            
            conn.close()
            print("Master.db extraction complete!")
        
        # Load all CSV files
        print("Loading metadata from CSV files...")
        metadata = {}
        
        for csv_file in metadata_dir.glob("*.csv"):
            table_name = csv_file.stem
            try:
                df = pd.read_csv(csv_file, encoding='utf-8')
                metadata[table_name] = df
                print(f"  Loaded {table_name}: {len(df)} rows")
            except Exception as e:
                print(f"  Error loading {table_name}: {e}")
        
        # Create lookup dictionaries for main tables
        print("Creating lookup dictionaries...")
        
        # Books lookup
        if 'book' in metadata:
            book_id_field = self._get_field_name(metadata['book'], 'book_id')
            metadata['books_lookup'] = metadata['book'].set_index(book_id_field).to_dict('index')
            print(f"  Created books lookup with field '{book_id_field}': {len(metadata['books_lookup'])} books")
        else:
            print("  WARNING: No 'book' table found!")
            metadata['books_lookup'] = {}
        
        # Authors lookup
        if 'author' in metadata:
            author_id_field = self._get_field_name(metadata['author'], 'author_id')
            metadata['authors_lookup'] = metadata['author'].set_index(author_id_field).to_dict('index')
            print(f"  Created authors lookup with field '{author_id_field}': {len(metadata['authors_lookup'])} authors")
        else:
            print("  WARNING: No 'author' table found!")
            metadata['authors_lookup'] = {}
        
        # Categories lookup
        if 'category' in metadata:
            category_id_field = self._get_field_name(metadata['category'], 'category_id')
            metadata['categories_lookup'] = metadata['category'].set_index(category_id_field).to_dict('index')
            print(f"  Created categories lookup with field '{category_id_field}': {len(metadata['categories_lookup'])} categories")
        else:
            print("  WARNING: No 'category' table found!")
            metadata['categories_lookup'] = {}
        
        # Load meta content from Lucene CSV files
        print("Loading Lucene meta content...")
        metadata['book_meta_lookup'] = self._load_meta_csv(self.csv_input_path / "book.csv")
        metadata['author_meta_lookup'] = self._load_meta_csv(self.csv_input_path / "author.csv")
        
        print(f"  Book meta lookup: {len(metadata['book_meta_lookup'])} entries")
        print(f"  Author meta lookup: {len(metadata['author_meta_lookup'])} entries")
        
        # Summary
        total_books = len(metadata.get('books_lookup', {}))
        total_authors = len(metadata.get('authors_lookup', {}))
        total_categories = len(metadata.get('categories_lookup', {}))
        
        print(f"Metadata loading complete:")
        print(f"  {total_books} books, {total_authors} authors, {total_categories} categories")
        
        return metadata
    
    def _load_meta_csv(self, csv_path):
        """Load meta content from CSV file"""
        if not csv_path.exists():
            return {}
        
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
            return {str(row["id"]): row["body_store"] 
                   for _, row in df.iterrows() 
                   if "id" in row and "body_store" in row 
                   and pd.notna(row["body_store"])}
        except Exception as e:
            print(f"  Warning: Could not load {csv_path.name}: {e}")
            return {}
    
    def _process_single_book(self, book_id, metadata):
        """Process a single book and return complete JSON structure"""
        
        # Get book metadata
        book_row = metadata['books_lookup'].get(int(book_id))
        
        if not book_row:
            return None
        
        # Load book content from CSV
        book_content_path = self.csv_input_path / "book_data" / f"{book_id}.csv"
        if not book_content_path.exists():
            return None
        
        try:
            book_content_df = pd.read_csv(book_content_path, encoding='utf-8')
            book_content = book_content_df.to_dict('records')
        except Exception as e:
            return None
        
        # Load book structure from individual book database
        last_three_digits = book_id[-3:].zfill(3)
        book_db_path = self.books_db_path / last_three_digits / f"{book_id}.db"
        page_structure = {}
        title_hierarchy = {}
        
        if book_db_path.exists():
            try:
                book_conn = sqlite3.connect(book_db_path)
                
                # Load page structure
                try:
                    page_df = pd.read_sql_query("SELECT * FROM page", book_conn)
                    page_structure = {str(row['id']): row.to_dict() 
                                    for _, row in page_df.iterrows()}
                except Exception:
                    pass
                
                # Load title hierarchy
                try:
                    title_df = pd.read_sql_query("SELECT * FROM title", book_conn)
                    for _, row in title_df.iterrows():
                        title_hierarchy[str(row['id'])] = str(row.get('parent', '0'))
                except Exception:
                    pass
                
                book_conn.close()
                
            except Exception:
                pass
        
        # Get meta content
        book_meta_content = []
        author_meta_content = []
        
        if book_id in metadata['book_meta_lookup']:
            book_meta_content = [line.strip() 
                               for line in metadata['book_meta_lookup'][book_id].split("\r") 
                               if line.strip()]
        
        main_author_id = str(book_row.get('main_author', ''))
        if main_author_id and main_author_id in metadata['author_meta_lookup']:
            author_meta_content = [line.strip() 
                                 for line in metadata['author_meta_lookup'][main_author_id].split("\r") 
                                 if line.strip()]
        
        # Build the complete book JSON
        book_json = self._build_book_json(
            book_id, book_row, book_content, page_structure, 
            title_hierarchy, metadata, book_meta_content, author_meta_content
        )
        
        return book_json
    
    def _build_book_json(self, book_id, book_data, book_content, page_structure, 
                        title_hierarchy, metadata, book_meta_content, author_meta_content):
        """Build the complete book JSON structure"""
        
        # Organize pages by part
        pages_by_part = defaultdict(list)
        
        for page_row in book_content:
            page_id = str(page_row.get('PageID', ''))
            structure = page_structure.get(page_id, {})
            
            # Apply footnote separation logic first
            raw_body = page_row.get('body', '')
            raw_foot = page_row.get('foot', '')
            separated_body, separated_footnote = self._separate_body_footnote(raw_body, raw_foot)
            
            # Store original body for TOC generation
            original_body = separated_body
            
            # Process text content
            body = self._process_text(separated_body, title_hierarchy)
            footnote = self._process_text(separated_footnote, title_hierarchy)
            
            part = structure.get('part', '') or ''
            page_number = structure.get('page', page_id)
            # Handle pandas float-to-int conversion and NULL values
            if pd.isna(page_number):
                page_number = ""  # Empty string for unknown page numbers
            elif isinstance(page_number, float):
                page_number = int(page_number)
                
            page_data = {
                "page_id": page_id,
                "page_number": str(page_number),
                "body": body,
                "footnote": footnote,
                "original_body": original_body  # Keep for TOC generation
            }
            
            pages_by_part[part].append(page_data)
        
        # Create parts list
        parts_list = []
        for part_name, pages in pages_by_part.items():
            parts_list.append({
                "part": part_name,
                "pages": pages
            })
        
        # Get category info
        category_id = book_data.get('book_category', '')
        category = metadata['categories_lookup'].get(category_id, {})
        
        # Get author info
        authors_info = []
        author_ids_str = str(book_data.get('authors', '')).strip()
        main_author_id = book_data.get('main_author', '')
        
        # Parse author IDs from string field
        if author_ids_str and author_ids_str not in ['', '[]', 'None']:
            author_ids = [aid.strip() for aid in author_ids_str.split(',') if aid.strip()]
        else:
            author_ids = []
        
        # Add main_author if not already in authors list
        if main_author_id and str(main_author_id) not in author_ids:
            author_ids.append(str(main_author_id))
        
        for author_id_str in author_ids:
            if author_id_str:
                try:
                    author_id_int = int(author_id_str)
                    author = metadata['authors_lookup'].get(author_id_int, {})
                    
                    if author:
                        authors_info.append({
                            "id": str(author_id_int),
                            "name": author.get('author_name', ''),
                            "death_number": author.get('death_number', ''),
                            "death_text": author.get('death_text', '') or '',
                            "is_main_author": author_id_int == main_author_id
                        })
                except ValueError:
                    continue
        
        # Generate table of contents if requested
        toc = self._build_table_of_contents(book_id, title_hierarchy, book_content, page_structure)
        
        # Clean up original_body from parts before output
        for part in parts_list:
            for page in part.get('pages', []):
                if 'original_body' in page:
                    del page['original_body']
        
        # Build final structure
        book_json = {
            "book_id": book_id,
            "title": book_data.get('book_name', ''),
            "book_date": book_data.get('book_date', ''),
            "category": {
                "id": category_id,
                "name": category.get('category_name', '')
            },
            "book_type": book_data.get('book_type', ''),
            "printed": book_data.get('printed', ''),
            "pdf_links": self._parse_json_field(book_data.get('pdf_links', '')) or {},
            "meta_data": self._parse_json_field(book_data.get('meta_data', '')) or {},
            "authors": authors_info,
            "book_meta": book_meta_content,
            "author_meta": author_meta_content,
            "parts": parts_list
        }
        
        # Add table of contents if generated
        if toc:
            book_json["table_of_contents"] = toc
        
        return book_json
    
    def _process_text(self, text, title_hierarchy):
        """Process text content by transforming span tags"""
        if not text or pd.isna(text):
            return ""
        
        # Ensure text is a string
        text = str(text)
        text = text.replace('\r', '\n')

        # Remove img tags
        text = re.sub(r'<img[^>]*/?>', '', text)
        
        def replace_title_span(match):
            title_id = match.group(1)
            title_content = match.group(2)
            parent_id = title_hierarchy.get(title_id, "0")
            return f'<title id={title_id} parent={parent_id}>{title_content}</title>'
        
        # Transform title spans
        text = re.sub(
            r'<span data-type=[\'"]title[\'"] id=toc-(\d+)>([\s\S]*?)</span>',
            replace_title_span, text
        )
        
        # Remove other span tags
        text = re.sub(r'<span[\s\S]*?>([\s\S]*?)</span>', r'\1', text)
        
        # Remove HTML tags when generating TOC
        if self.generate_toc:
            # Remove title tags but keep content
            text = re.sub(r'<title[^>]*>([\s\S]*?)</title>', r'\1', text)
            # Remove any remaining HTML tags
            text = re.sub(r'<[^>]+>', '', text)
        
        return text
    
    def _build_table_of_contents(self, book_id, title_hierarchy, book_content, page_structure):
        """Build table of contents in the user's proven structure"""
        if not self.generate_toc:
            return None
        
        # Load title data from CSV (just title text)
        title_data = self._load_title_data(book_id)
        if not title_data:
            return None
        
        # Build title-to-page mapping from SQLite database
        title_page_mapping = self._build_title_page_mapping_from_sqlite(book_id, page_structure)
        
        # Build hierarchical TOC structure using Python-built mapping
        toc_items = []
        processed_titles = set()
        
        for title_id, title_text in title_data.items():
            if title_id in processed_titles:
                continue
            
            parent_id = title_hierarchy.get(title_id, "0")
            
            if parent_id == "0":  # Root level item
                page_number = title_page_mapping.get(title_id, "1")
                toc_item = {
                    "page": f"{book_id}/{page_number}",
                    "title": title_text.strip(),
                    "chapters": []
                }
                
                # Find children
                children = self._find_title_children(title_id, title_hierarchy, title_data, title_page_mapping, book_id)
                toc_item["chapters"] = children
                
                toc_items.append(toc_item)
                processed_titles.add(title_id)
                
                # Mark children as processed
                self._mark_children_processed(title_id, title_hierarchy, processed_titles)
        
        return toc_items
    
    def _find_title_children(self, parent_id, title_hierarchy, title_data, title_page_mapping, book_id):
        """Recursively find children titles"""
        children = []
        
        for title_id, title_text in title_data.items():
            if title_hierarchy.get(title_id, "0") == parent_id:
                page_number = title_page_mapping.get(title_id, "1")
                child_item = {
                    "page": f"{book_id}/{page_number}",
                    "title": title_text.strip(),
                    "chapters": []
                }
                
                # Recursively find grandchildren
                grandchildren = self._find_title_children(title_id, title_hierarchy, title_data, title_page_mapping, book_id)
                child_item["chapters"] = grandchildren
                
                children.append(child_item)
        
        return children
    
    def _mark_children_processed(self, parent_id, title_hierarchy, processed_titles):
        """Mark all children as processed recursively"""
        for title_id in title_hierarchy:
            if title_hierarchy.get(title_id, "0") == parent_id:
                processed_titles.add(title_id)
                self._mark_children_processed(title_id, title_hierarchy, processed_titles)
    
    def _separate_body_footnote(self, body, foot):
        """
        Smart footnote separation logic:
        - If body is empty and foot contains _____+ pattern: split foot into body and footnote
        - If body is empty and foot has no pattern: move foot to body, empty footnote
        - If body is not empty: keep original (body=body, footnote=foot)
        """
        # Ensure strings
        body = str(body) if body and not pd.isna(body) else ""
        foot = str(foot) if foot and not pd.isna(foot) else ""
        
        # Clean up whitespace
        body = body.strip()
        foot = foot.strip()
        
        # If body is not empty, use original logic
        if body:
            return body, foot
        
        # If body is empty, apply separation logic
        if not body and foot:
            # Look for sequence of 5 or more underscores as separator
            separator_pattern = r'_{5,}'
            if re.search(separator_pattern, foot):
                # Split on first occurrence of 5+ underscores
                parts = re.split(separator_pattern, foot, 1)
                return parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""
            else:
                # No separator: foot is actually the body
                return foot, ""
        
        # Both empty
        return body, foot
    
    def _parse_json_field(self, json_str):
        """Parse JSON string field from database"""
        if not json_str or pd.isna(json_str) or json_str.strip() == '':
            return None
        
        try:
            json_str = str(json_str).strip()
            parsed = json.loads(json_str)
            return parsed
        except (json.JSONDecodeError, ValueError):
            return None
    
    def _show_json_summary(self):
        """Show summary of created JSON files"""
        print("\n" + "="*40)
        print("JSON FILES SUMMARY")
        print("="*40)
        
        json_files = list(self.json_output_dir.glob("*.json"))
        total_size = sum(f.stat().st_size for f in json_files)
        
        print(f"Files created: {len(json_files):,}")
        print(f"Total size: {total_size / (1024**3):.2f} GB")
        if json_files:
            print(f"Average per file: {total_size / len(json_files) / 1024:.1f} KB")
        print(f"Location: {self.json_output_dir}")
    
    def _load_title_data(self, book_id):
        """Load title data for the book from CSV file"""
        title_path = self.csv_input_path / "title_data" / f"title_{book_id}.csv" 
        if not title_path.exists():
            return {}
        
        try:
            df = pd.read_csv(title_path, encoding='utf-8')
            # Return mapping of numeric_title_id -> title_text
            # Extract numeric part from id like "1-8" -> "8"
            title_mapping = {}
            for _, row in df.iterrows():
                if "id" in row and "body" in row and pd.notna(row["body"]):
                    full_id = str(row["id"])
                    # Extract numeric part after the dash (e.g., "1-8" -> "8")
                    if '-' in full_id:
                        numeric_id = full_id.split('-')[1]
                        title_mapping[numeric_id] = row["body"]
            return title_mapping
        except Exception as e:
            print(f"  Warning: Could not load title data for book {book_id}: {e}")
            return {}
    
    def _build_title_page_mapping_from_sqlite(self, book_id, page_structure):
        """Build mapping of title_id to actual_page_number using SQLite database"""
        title_page_mapping = {}
        
        # Get book database path
        last_three_digits = book_id[-3:].zfill(3)
        book_db_path = self.books_db_path / last_three_digits / f"{book_id}.db"
        
        if not book_db_path.exists():
            print(f"  Warning: No database found for book {book_id} at {book_db_path}")
            return title_page_mapping
        
        try:
            import sqlite3
            conn = sqlite3.connect(book_db_path)
            
            # Query the title table to get title_id -> page_id mapping
            cursor = conn.execute("SELECT id, page FROM title")
            
            for title_id, page_id in cursor.fetchall():
                # Convert to strings for consistency
                title_id_str = str(title_id)
                page_id_str = str(page_id)
                
                # Use page ID directly for TOC URLs, not printed page numbers
                title_page_mapping[title_id_str] = page_id_str
            
            conn.close()
            
        except Exception as e:
            print(f"  Warning: Could not load title mapping from SQLite for book {book_id}: {e}")
        
        return title_page_mapping
    
    def _get_field_name(self, df, base_name):
        """Handle BOM characters in field names"""
        if base_name in df.columns:
            return base_name
        elif f"\ufeff{base_name}" in df.columns:
            return f"\ufeff{base_name}"
        else:
            return base_name
    
    def _log(self, log_file, message):
        """Write message to log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            print(f"Warning: Could not write to log: {e}")


def main():
    """Main function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Build JSON files from Shamela extraction')
    parser.add_argument('--test-single-book', action='store_true', 
                       help='Test mode: process only the first book found')
    parser.add_argument('--generate-toc', action='store_true',
                       help='Generate table of contents and remove HTML tags from body text')
    parser.add_argument('--shamela-path', type=str, default='C:/shamela4',
                       help='Path to Shamela base directory (default: C:/shamela4)')
    parser.add_argument('--extracted-path', type=str, default='.',
                       help='Path to extracted data directory (default: current directory)')
    
    args = parser.parse_args()
    
    print("SHAMELA JSON BUILDER")
    print("=" * 50)
    
    if args.test_single_book:
        print("TEST MODE: Will process only first book")
        print("=" * 50)
    
    if args.generate_toc:
        print("GENERATING TABLE OF CONTENTS - HTML tags will be removed from body text")
        print("=" * 50)
    
    # Use command line args or prompt for interactive mode
    if len(sys.argv) == 1:  # No command line args, interactive mode
        shamela_path = input("Enter Shamela base path (e.g., C:/shamela4): ").strip()
        if not shamela_path:
            shamela_path = "C:/shamela4"
            print(f"Using default: {shamela_path}")
        
        extracted_path = input("Enter extracted data path (default: current directory): ").strip()
        if not extracted_path:
            extracted_path = os.getcwd()
            print(f"Using default: {extracted_path}")
        
        test_mode = False
        generate_toc = False
    else:
        shamela_path = args.shamela_path
        extracted_path = args.extracted_path if args.extracted_path != '.' else os.getcwd()
        test_mode = args.test_single_book
        generate_toc = args.generate_toc
        
        print(f"Shamela path: {shamela_path}")
        print(f"Extracted data path: {extracted_path}")
    
    print()
    
    # Create builder
    builder = JSONBuilder(shamela_path, extracted_path, test_mode=test_mode, generate_toc=generate_toc)
    
    # Build JSON files
    success = builder.build_json_files()
    
    # Final summary
    if success:
        print("\n" + "="*60)
        print("BUILDING COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("Output files created:")
        print(f"- JSON files: {builder.json_output_dir}")
        print(f"- Logs: {builder.log_dir}")
    else:
        print("\n" + "="*60)
        print("BUILDING FAILED!")
        print("="*60)
        print("Please check:")
        print("1. Prerequisites are satisfied")
        print("2. CSV files exist (run extract_indices.py first)")
        print(f"3. Log files in: {builder.log_dir}")


if __name__ == "__main__":
    main()