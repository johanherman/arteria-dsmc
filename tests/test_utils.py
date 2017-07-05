
class TestUtils:

    DUMMY_CONFIG = {
        "monitored_directory": "tests/resources/",
        "md5_log_directory": "/tmp"
    }

class DummyConfig:
    def __getitem__(self, key):
        return TestUtils.DUMMY_CONFIG[key]

