"""
Test suite for rugo parquet metadata decoder.
"""

import pytest
import tempfile
import io
import struct
from pathlib import Path

# Import after building
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestParquetDecoder:
    
    @pytest.fixture
    def minimal_parquet_bytes(self):
        """Create minimal parquet bytes for testing basic parsing."""
        # Create a minimal parquet file structure
        # This is a very basic structure just for testing the parser
        
        # Parquet magic number at the start
        header = b"PAR1"
        
        # Minimal footer structure
        # In real parquet files, this would be proper Thrift-encoded metadata
        fake_metadata = b'{"version": 1, "num_rows": 100}'
        
        # Footer: metadata_length (4 bytes) + magic number (4 bytes)
        footer_length = len(fake_metadata)
        footer = struct.pack('<I', footer_length) + b"PAR1"
        
        # Combine: header + fake_metadata + footer
        return header + fake_metadata + footer
    
    @pytest.fixture
    def invalid_parquet_bytes(self):
        """Create invalid parquet bytes for error testing."""
        return b"not parquet data"
    
    @pytest.fixture
    def parquet_stream(self, minimal_parquet_bytes):
        """Create a parquet stream from minimal bytes."""
        return io.BytesIO(minimal_parquet_bytes)
    
    @pytest.fixture
    def parquet_file(self, minimal_parquet_bytes):
        """Create a temporary parquet file."""
        temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        temp_file.write(minimal_parquet_bytes)
        temp_file.close()
        
        yield temp_file.name
        
        # Cleanup
        Path(temp_file.name).unlink(missing_ok=True)
    
    def test_decoder_with_stream(self, parquet_stream):
        """Test ParquetDecoder with stream input."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test metadata loading
        decoder.load_metadata()
        metadata = decoder.get_metadata()
        
        assert isinstance(metadata, dict)
        assert 'file_size' in metadata
        assert 'version' in metadata
        
        decoder.close()
    
    def test_decoder_with_bytes(self, minimal_parquet_bytes):
        """Test ParquetDecoder with bytes input."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(minimal_parquet_bytes)
        
        metadata = decoder.get_metadata()
        assert isinstance(metadata, dict)
        assert metadata['file_size'] == len(minimal_parquet_bytes)
        
        decoder.close()
    
    def test_decoder_with_file_path(self, parquet_file):
        """Test ParquetDecoder with file path."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_file)
        
        metadata = decoder.get_metadata()
        assert isinstance(metadata, dict)
        assert metadata['file_size'] > 0
        
        decoder.close()
    
    def test_get_column_names(self, parquet_stream):
        """Test column name extraction (placeholder implementation)."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Column names are empty in placeholder implementation
        columns = decoder.get_column_names()
        assert isinstance(columns, list)
        # Without full Thrift parsing, this returns empty list
        assert columns == []
        
        decoder.close()
    
    def test_get_statistics(self, parquet_stream):
        """Test statistics extraction (placeholder implementation)."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Statistics are placeholder in current implementation
        stats = decoder.get_statistics('test_column')
        assert isinstance(stats, dict)
        assert 'column_name' in stats
        assert stats['available'] == False
        
        decoder.close()
    
    def test_get_bloom_filters(self, parquet_stream):
        """Test bloom filter extraction (placeholder implementation)."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Bloom filters are placeholder in current implementation
        bloom_filters = decoder.get_bloom_filters('test_column')
        assert isinstance(bloom_filters, dict)
        
        decoder.close()
    
    def test_wrapper_methods(self, parquet_stream):
        """Test row group wrapper methods."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test all wrapper methods
        all_stats = decoder.get_all_statistics()
        assert isinstance(all_stats, dict)
        
        all_bloom_filters = decoder.get_all_bloom_filters()
        assert isinstance(all_bloom_filters, dict)
        
        results = decoder.check_bloom_filter_all_row_groups('test_column', 'test_value')
        assert isinstance(results, list)
        
        row_group_stats = decoder.get_row_group_statistics()
        assert isinstance(row_group_stats, list)
        
        decoder.close()
    
    def test_convenience_functions(self, parquet_stream, minimal_parquet_bytes, parquet_file):
        """Test module-level convenience functions."""
        from rugo.decoders.parquet_decoder import (
            get_parquet_info, 
            get_parquet_statistics,
            get_parquet_bloom_filters
        )
        
        # Test with stream
        info = get_parquet_info(parquet_stream)
        assert isinstance(info, dict)
        assert 'file_size' in info
        
        # Test with bytes
        info = get_parquet_info(minimal_parquet_bytes)
        assert isinstance(info, dict)
        
        # Test with file path
        info = get_parquet_info(parquet_file)
        assert isinstance(info, dict)
        
        # Test statistics function
        stats = get_parquet_statistics(parquet_stream, 'test_column')
        assert isinstance(stats, dict)
        
        # Test bloom filters function
        bloom_filters = get_parquet_bloom_filters(parquet_stream, 'test_column')
        assert isinstance(bloom_filters, dict)
    
    def test_error_handling(self, invalid_parquet_bytes):
        """Test error handling with invalid data."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        # Test with invalid bytes
        decoder = ParquetDecoder(invalid_parquet_bytes)
        
        with pytest.raises(ValueError):
            decoder.load_metadata()
        
        # Test with empty stream
        empty_stream = io.BytesIO()
        decoder = ParquetDecoder(empty_stream)
        
        with pytest.raises(ValueError):
            decoder.load_metadata()
    
    def test_check_bloom_filter(self, parquet_stream):
        """Test individual bloom filter checking."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Should always return True in placeholder implementation
        result = decoder.check_bloom_filter('test_column', 'test_value')
        assert result == True
        
        decoder.close()


if __name__ == "__main__":
    pytest.main([__file__])