"""
Test suite for rugo parquet decoder.
"""

import pytest
import tempfile
import array
import io
import random
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Import after building
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestParquetDecoder:
    
    @pytest.fixture
    def sample_data(self):
        """Create sample parquet data for testing."""
        random.seed(42)
        
        # Create test data
        num_rows = 1000
        data = {
            'id': pa.array(list(range(num_rows))),
            'name': pa.array([f'item_{i}' for i in range(num_rows)]),
            'price': pa.array([random.uniform(10, 1000) for _ in range(num_rows)]),
            'category': pa.array([random.choice(['A', 'B', 'C']) for _ in range(num_rows)]),
            'in_stock': pa.array([random.choice([True, False]) for _ in range(num_rows)]),
        }
        
        table = pa.table(data)
        return table
    
    @pytest.fixture
    def parquet_stream(self, sample_data):
        """Create a parquet stream from sample data."""
        stream = io.BytesIO()
        pq.write_table(sample_data, stream)
        stream.seek(0)
        return stream
    
    @pytest.fixture
    def parquet_bytes(self, sample_data):
        """Create parquet bytes from sample data."""
        stream = io.BytesIO()
        pq.write_table(sample_data, stream)
        return stream.getvalue()
    
    @pytest.fixture
    def parquet_file(self, sample_data):
        """Create a temporary parquet file from sample data."""
        temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        temp_file.close()
        
        pq.write_table(sample_data, temp_file.name)
        yield temp_file.name
        
        # Cleanup
        Path(temp_file.name).unlink(missing_ok=True)
    
    def test_decoder_with_stream(self, parquet_stream):
        """Test ParquetDecoder with stream input."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test metadata
        metadata = decoder.get_metadata()
        assert metadata['num_rows'] == 1000
        assert metadata['num_columns'] == 5
        
        # Test column names
        columns = decoder.get_column_names()
        assert set(columns) == {'id', 'name', 'price', 'category', 'in_stock'}
        
        # Test reading columns
        table = decoder.read_columns(['id', 'price'])
        assert table.shape == (1000, 2)
        
        decoder.close()
    
    def test_decoder_with_bytes(self, parquet_bytes):
        """Test ParquetDecoder with bytes input."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_bytes)
        
        metadata = decoder.get_metadata()
        assert metadata['num_rows'] == 1000
        assert metadata['num_columns'] == 5
        
        decoder.close()
    
    def test_decoder_with_file_path(self, parquet_file):
        """Test ParquetDecoder with file path (legacy support)."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_file)
        
        metadata = decoder.get_metadata()
        assert metadata['num_rows'] == 1000
        assert metadata['num_columns'] == 5
        
        decoder.close()
    
    def test_fast_numeric_read(self, parquet_stream):
        """Test fast numeric column reading returning array.array."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test numeric column
        prices = decoder.read_numeric_column_fast('price')
        assert isinstance(prices, array.array)
        assert len(prices) == 1000
        assert prices.typecode in ['d', 'q']  # double or long long
        
        # Test integer column
        ids = decoder.read_numeric_column_fast('id')
        assert isinstance(ids, array.array)
        assert len(ids) == 1000
        
        decoder.close()
    
    def test_statistics(self, parquet_stream):
        """Test statistics extraction."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test individual column statistics
        price_stats = decoder.get_statistics('price')
        assert 'min' in price_stats
        assert 'max' in price_stats
        assert 'null_count' in price_stats
        
        # Test all statistics
        all_stats = decoder.get_all_statistics()
        assert isinstance(all_stats, dict)
        assert len(all_stats) == 5
        assert 'price' in all_stats
        
        decoder.close()
    
    def test_bloom_filters(self, parquet_stream):
        """Test bloom filter functionality."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test individual column bloom filters
        bloom_filters = decoder.get_bloom_filters('category')
        assert isinstance(bloom_filters, dict)
        
        # Test all bloom filters
        all_bloom_filters = decoder.get_all_bloom_filters()
        assert isinstance(all_bloom_filters, dict)
        assert len(all_bloom_filters) == 5
        
        # Test bloom filter checking across all row groups
        results = decoder.check_bloom_filter_all_row_groups('category', 'A')
        assert isinstance(results, list)
        assert len(results) >= 1  # At least one row group
        
        decoder.close()
    
    def test_row_group_statistics(self, parquet_stream):
        """Test row group statistics functionality."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        row_group_stats = decoder.get_row_group_statistics()
        assert isinstance(row_group_stats, list)
        assert len(row_group_stats) >= 1
        
        for stats in row_group_stats:
            assert 'row_group_idx' in stats
            assert 'num_rows' in stats
            assert 'num_columns' in stats
            assert 'columns' in stats
            assert isinstance(stats['columns'], dict)
        
        decoder.close()
    
    def test_convenience_functions(self, parquet_stream, parquet_bytes, parquet_file):
        """Test module-level convenience functions."""
        from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info
        
        # Test with stream
        table = read_parquet(parquet_stream)
        assert table.shape == (1000, 5)
        
        # Test with bytes
        table = read_parquet(parquet_bytes)
        assert table.shape == (1000, 5)
        
        # Test with file path
        table = read_parquet(parquet_file)
        assert table.shape == (1000, 5)
        
        # Test get_parquet_info
        info = get_parquet_info(parquet_stream)
        assert info['num_rows'] == 1000
        assert info['num_columns'] == 5
    
    def test_selective_column_reading(self, parquet_stream):
        """Test reading specific columns."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        decoder = ParquetDecoder(parquet_stream)
        
        # Test reading subset of columns
        table = decoder.read_columns(['id', 'name'])
        assert table.shape == (1000, 2)
        assert table.column_names == ['id', 'name']
        
        decoder.close()
    
    def test_error_handling(self):
        """Test error handling with invalid data."""
        from rugo.decoders.parquet_decoder import ParquetDecoder
        
        # Test with invalid bytes
        invalid_data = b"not parquet data"
        decoder = ParquetDecoder(invalid_data)
        
        with pytest.raises(ValueError):
            decoder.load_file()
        
        # Test with empty stream
        empty_stream = io.BytesIO()
        decoder = ParquetDecoder(empty_stream)
        
        with pytest.raises(ValueError):
            decoder.load_file()


if __name__ == "__main__":
    pytest.main([__file__])