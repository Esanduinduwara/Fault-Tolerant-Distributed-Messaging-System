# conftest.py — placed at the project root so pytest can find src/ as a package
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
