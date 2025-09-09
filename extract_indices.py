import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import platform
import argparse

class LuceneIndexExtractor:
    def __init__(self, shamela_base_path, output_base_path, test_mode=False):
        """
        Initialize the Lucene index extractor
        
        Args:
            shamela_base_path: Path to shamela4 directory (e.g., "C:/shamela4")
            output_base_path: Path where CSV outputs will be saved
            test_mode: If True, process only the first book found
        """
        self.test_mode = test_mode
        self.shamela_base_path = Path(shamela_base_path)
        self.output_base_path = Path(output_base_path)
        
        # Key paths
        self.lucene_store_path = self.shamela_base_path / "database" / "store"
        self.csv_output_path = self.output_base_path / "exported_indices"
        self.log_dir = self.output_base_path / "logs"
        
        # Create output directories
        self.output_base_path.mkdir(parents=True, exist_ok=True)
        self.csv_output_path.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)
        
        # Platform-specific classpath separator
        self.classpath_sep = ';' if platform.system() == 'Windows' else ':'
        
        # Log file
        self.log_file = self.log_dir / "lucene_extraction.log"
    
    def check_prerequisites(self):
        """Check if all required files and dependencies are available"""
        print("Checking prerequisites...")
        
        issues = []
        
        # Check Java installation
        try:
            result = subprocess.run(['java', '-version'], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                issues.append("Java is not installed or not in PATH")
            else:
                java_version = result.stderr.split('\n')[0]
                print(f"Java found: {java_version}")
        except FileNotFoundError:
            issues.append("Java is not installed or not in PATH")
        
        # Check Shamela lucene store path
        if not self.lucene_store_path.exists():
            issues.append(f"Shamela Lucene store not found: {self.lucene_store_path}")
        else:
            print(f"Shamela Lucene store found: {self.lucene_store_path}")
        
        # Check Java class file
        java_class = Path("java/ShamelaIndexExporter.class")
        if not java_class.exists():
            issues.append("ShamelaIndexExporter.class not found /java/ directory")
        else:
            print(f"Java class file found: {java_class}")
        
        # Check Lucene JAR files
        lib_dir = Path("lib")
        if not lib_dir.exists():
            issues.append("lib/ directory not found")
        else:
            lucene_jars = list(lib_dir.glob("lucene-core-*.jar"))
            if not lucene_jars:
                issues.append("No lucene-core-*.jar files found in lib/ directory")
            else:
                print(f"Lucene JAR found: {lucene_jars[0].name}")
        
        # Check available disk space
        try:
            free_space = self._get_free_space(self.output_base_path)
            if free_space < 20 * 1024 * 1024 * 1024:  # 20 GB
                issues.append(f"Low disk space: {free_space / (1024**3):.1f} GB free, recommend 20+ GB")
            else:
                print(f"Disk space: {free_space / (1024**3):.1f} GB available")
        except:
            print("Could not check disk space")
        
        if issues:
            print("\nIssues found:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print("All prerequisites satisfied!")
        return True
    
    def _get_free_space(self, path):
        """Get free disk space in bytes"""
        if platform.system() == 'Windows':
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(str(path)), ctypes.pointer(free_bytes), None, None
            )
            return free_bytes.value
        else:
            statvfs = os.statvfs(path)
            return statvfs.f_frsize * statvfs.f_bavail
    
    def extract_indices(self):
        """Run the Java Lucene extractor"""
        print("="*60)
        print("EXTRACTING LUCENE INDICES TO CSV FILES")
        print("="*60)
        
        if not self.check_prerequisites():
            print("\nCannot proceed due to missing prerequisites.")
            return False
        
        # Prepare Java command
        classpath = f"lib/*{self.classpath_sep}java{self.classpath_sep}."
        java_cmd = [
            'java',
            '-cp', classpath,
            'ShamelaIndexExporter',
            str(self.lucene_store_path),
            str(self.csv_output_path)
        ]
        
        # Add test mode parameter if enabled
        if self.test_mode:
            java_cmd.append('--test-single-book')
        
        print(f"\nStarting extraction...")
        print(f"Source: {self.lucene_store_path}")
        print(f"Output: {self.csv_output_path}")
        print(f"Command: {' '.join(java_cmd)}")
        print()
        
        # Log start time
        start_time = datetime.now()
        self._log(f"Starting Lucene extraction at {start_time}")
        self._log(f"Command: {' '.join(java_cmd)}")
        
        try:
            # Run Java extractor with real-time output
            process = subprocess.Popen(
                java_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Stream output in real-time
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    self._log(f"Java: {output.strip()}")
            
            # Wait for completion
            return_code = process.poll()
            
            if return_code == 0:
                end_time = datetime.now()
                duration = end_time - start_time
                
                print(f"\nExtraction completed successfully!")
                print(f"Duration: {duration}")
                
                # Show output summary
                self._show_output_summary()
                
                self._log(f"Extraction completed successfully at {end_time}")
                self._log(f"Duration: {duration}")
                
                return True
            else:
                print(f"\nJava extraction failed with return code: {return_code}")
                self._log(f"Java extraction failed with return code: {return_code}")
                return False
                
        except Exception as e:
            print(f"\nError running Java extractor: {str(e)}")
            self._log(f"Error running Java extractor: {str(e)}")
            return False
    
    def _show_output_summary(self):
        """Show summary of extracted files"""
        print("\n" + "="*40)
        print("EXTRACTION SUMMARY")
        print("="*40)
        
        if not self.csv_output_path.exists():
            print("No output directory found")
            return
        
        # Count files and calculate sizes
        total_size = 0
        file_counts = {}
        
        for item in self.csv_output_path.rglob("*"):
            if item.is_file():
                size = item.stat().st_size
                total_size += size
                
                # Categorize files
                if item.parent.name == "book_data":
                    file_counts["Individual book CSVs"] = file_counts.get("Individual book CSVs", 0) + 1
                elif item.parent.name == "title_data":
                    file_counts["Title CSVs"] = file_counts.get("Title CSVs", 0) + 1
                elif item.suffix == ".csv":
                    file_counts["Index CSVs"] = file_counts.get("Index CSVs", 0) + 1
        
        # Display summary
        for category, count in file_counts.items():
            print(f"{category}: {count:,} files")
        
        print(f"\nTotal size: {total_size / (1024**3):.2f} GB")
        print(f"Output location: {self.csv_output_path}")
        
        # Check for expected key files
        expected_files = [
            self.csv_output_path / "author.csv",
            self.csv_output_path / "book.csv",
            self.csv_output_path / "book_data"
        ]
        
        print(f"\nKey files check:")
        for expected in expected_files:
            if expected.exists():
                if expected.is_file():
                    size = expected.stat().st_size / (1024**2)
                    print(f"{expected.name} ({size:.1f} MB)")
                else:
                    count = len(list(expected.glob("*")))
                    print(f"{expected.name}/ ({count:,} files)")
            else:
                print(f"{expected.name} - NOT FOUND")
    
    def _log(self, message):
        """Write message to log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            print(f"Warning: Could not write to log file: {e}")


def main():
    """Main function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract Lucene indices from Shamela to CSV files')
    parser.add_argument('--test-single-book', action='store_true',
                       help='Test mode: process only the first book found')
    parser.add_argument('--shamela-path', type=str, default='C:/shamela4',
                       help='Path to Shamela base directory (default: C:/shamela4)')
    parser.add_argument('--output-path', type=str, default='.',
                       help='Path to output directory (default: current directory)')
    
    args = parser.parse_args()
    
    print("SHAMELA LUCENE INDEX EXTRACTOR")
    print("=" * 50)
    
    if args.test_single_book:
        print("TEST MODE: Will process only first book found")
        print("=" * 50)
    
    # Use command line args or prompt for interactive mode
    if len(sys.argv) == 1:  # No command line args, interactive mode
        shamela_path = input("Enter Shamela base path (e.g., C:/shamela4): ").strip()
        if not shamela_path:
            shamela_path = "C:/shamela4"
            print(f"Using default: {shamela_path}")
            
        default_output = os.getcwd()
        output_path = input(f"Enter output path (default: {default_output}): ").strip()
        if not output_path:
            output_path = default_output
            print(f"Using default: {output_path}")
        
        test_mode = False
    else:
        shamela_path = args.shamela_path
        output_path = args.output_path if args.output_path != '.' else os.getcwd()
        test_mode = args.test_single_book
        
        print(f"Shamela path: {shamela_path}")
        print(f"Output path: {output_path}")
    
    print()
    
    # Create extractor and run
    extractor = LuceneIndexExtractor(shamela_path, output_path, test_mode=test_mode)
    
    success = extractor.extract_indices()
    
    if success:
        print("\n" + "="*60)
        print("EXTRACTION COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("Next steps:")
        print("1. Check the output files in:", extractor.csv_output_path)
        print("2. Run build_jsonls.py to create JSONL files")
        print(f"3. Check logs in: {extractor.log_file}")
    else:
        print("\n" + "="*60)
        print("EXTRACTION FAILED!")
        print("="*60)
        print("Please check:")
        print("1. Prerequisites are installed")
        print("2. Paths are correct")
        print(f"3. Log file for details: {extractor.log_file}")
        sys.exit(1)


if __name__ == "__main__":
    main()