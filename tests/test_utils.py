
class TestUtils:

    DUMMY_CONFIG = {
        "monitored_directory": "tests/resources/",
        "whitelisted_warnings": "[\"ANS1809W\", \"ANS2000W\"]", 
        "dsmc_log_directory": "tests/resources/dsmc_output/", 
        "path_to_archive_root": "tests/resources/archives/"
    }

class DummyConfig:
    def __getitem__(self, key):
        return TestUtils.DUMMY_CONFIG[key]

