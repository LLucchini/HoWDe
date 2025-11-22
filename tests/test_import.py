# tests/test_import.py
def test_import():
    import howde
    assert hasattr(howde, "__version__")

