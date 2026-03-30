import sys
import os

# Make processing-engine importable without installing it as a package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processing-engine"))
