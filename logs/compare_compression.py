import math
from collections import Counter
import os
import subprocess
import glob

def file_entropy(filepath):
    """Calculate Shannon entropy of a file (bits per byte)"""
    with open(filepath, 'rb') as f:
        data = f.read()
    
    if not data:
        return 0.0
    
    counts = Counter(data)
    length = len(data)
    
    entropy = 0.0
    for count in counts.values():
        p = count / length
        entropy -= p * math.log2(p)
    
    return entropy

# Find all .log files
log_files = glob.glob('*.log')

print("Compression Efficiency Comparison")
print("=" * 85)

for original in sorted(log_files):
    print(f"\n{original}")
    print("-" * 85)
    
    # Compress with gzip and zstd
    gz_file = original + '.gz'
    zst_file = original + '.zst'
    
    # Gzip (if doesn't exist)
    if not os.path.exists(gz_file):
        subprocess.run(['gzip', '-k', '-f', original], check=True, capture_output=True)
    
    # Zstd
    subprocess.run(['zstd', '-f', '-q', original, '-o', zst_file], check=True, capture_output=True)
    
    # Analyze
    original_size = os.path.getsize(original)
    
    print(f"{'Format':<15} {'Size (KB)':<12} {'Entropy':<12} {'Ratio':<10}")
    print("-" * 85)
    
    for filepath, label in [(original, 'Original'), (gz_file, 'Gzip'), (zst_file, 'Zstd')]:
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            size_kb = size / 1024
            h = file_entropy(filepath)
            ratio = original_size / size
            print(f"{label:<15} {size_kb:>8.1f} KB   {h:.4f} bits   {ratio:.2f}x")
    
    # Cleanup zstd file
    os.remove(zst_file)

print("\n" + "=" * 85)
print("Note: Higher ratio = better compression")
