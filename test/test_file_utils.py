import pytest
from src.utils.file_utils import convert_to_dict, get_path_date_time_string


@pytest.fixture
def query_results():
    db_values = [
        (2, "test_data2", "1030-01-01T00:00:00+00:00"),
        (3, "test_data3", "1940-01-01T00:00:00+00:00"),
        (4, "test_data4", "1001-01-01T00:00:00+00:00"),
        (5, "test_data5", "1070-01-01T00:00:00+00:00"),
        (6, "test_data6", "1005-01-01T00:00:00+00:00"),
        (7, "test_data7", "1080-01-01T00:00:00+00:00"),
        (8, "test_data8", "1031-01-01T00:00:00+00:00"),
    ]

    return db_values


@pytest.fixture
def column_names():
    test_column_names = ("id", "example_col", "test_data")
    return test_column_names


class TestConvertToDict:
    """
    Test suite for the convert_to_dic function.
    """

    def test_returns_correct_format(self, query_results, column_names):
        value_json_compat = convert_to_dict(query_results, column_names)
        assert type(value_json_compat) is type([])
        assert type(value_json_compat[0]) is type({})

    def test_does_not_mutate_data(self, query_results, column_names):
        query_results_before = query_results
        convert_to_dict(query_results, column_names)
        query_results_after = query_results

        assert query_results_before == query_results_after
        assert query_results_before is query_results_after

    def test_col_names_are_keys_for_new_data(self, query_results, column_names):
        values_json_compat = convert_to_dict(query_results, column_names)
        for i, dict in enumerate(values_json_compat):
            count = 0
            for key, value in dict.items():
                query_value = query_results[i][count]
                json_converted_value = value
                assert key == column_names[count]
                count += 1
                assert query_value == json_converted_value


class TestGetPathDateTimeString:

    def test_output_is_string(self):
        date_time_str = get_path_date_time_string()
        assert type(date_time_str) is str

    def test_output_has_no_chars_that_effect_file_paths(self):
        date_time_str = get_path_date_time_string()
        assert "/" not in date_time_str
