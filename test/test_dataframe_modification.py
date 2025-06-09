from src.transform_lambda.dataframe_modification import dataframe_modification
import pandas as pd
import pytest

string_1 = {
    "sales_order_id": [2, 3],
    "created_at": ["2022-11-03 14:20:52.186000", "2022-11-03 14:20:52.188000"],
    "last_updated": ["2022-11-03 14:20:52.186000", "2022-11-03 14:20:52.188000"],
    "design_id": [3, 4],
    "staff_id": [19, 10],
    "counterparty_id": [8, 4],
    "units_sold": [42972, 65839],
    "unit_price": ["3.94", "2.91"],
    "currency_id": [2, 3],
    "agreed_delivery_date": ["2022-11-07", "2022-11-06"],
    "agreed_payment_date": ["2022-11-08", "2022-11-07"],
    "agreed_delivery_location_id": [8, 19],
}

dataframe_1 = pd.DataFrame(string_1)

test_dict_list = [{"sales_order": dataframe_1}]


def staff_df():
    column_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ]
    staff_df = pd.DataFrame(columns=column_name_list)
    for i in range(10):
        data = {
            column_name_list[0]: i,
            column_name_list[1]: f"first_name_{i}",
            column_name_list[2]: f"last_name_{i}",
            column_name_list[3]: i,
            column_name_list[4]: f"email_address{i}",
            column_name_list[5]: i,
            column_name_list[6]: i,
        }
        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        staff_df = pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)
    return staff_df


staff_data = staff_df()


def test_dataframe_modification_returns_list_of_dicts():
    test_dict_list = [{"sales_order": dataframe_1}]
    test_dict_list_2 = [{"sales_order": dataframe_1}, {"staff": staff_data}]
    assert type(dataframe_modification(test_dict_list)) == list
    assert type(dataframe_modification(test_dict_list_2)) == list

def test_datafram_modification_has_correct_table_keys():
    test_dict_list = [{"sales_order": dataframe_1}]
    test_staff_list = [{"staff": staff_data}]
    assert "sales_order" in dataframe_modification(test_dict_list)[0].keys()
    assert "staff" in dataframe_modification(test_staff_list)[0].keys()

def test_datafram_modification_has_correct_table_keys_for_multiple_items():
    test_dict_list = [{"sales_order": dataframe_1}, {"staff": staff_data}]
    table_names=[]
    for item in dataframe_modification(test_dict_list):
        for key in item.keys():
            table_names.append(key)
    assert "sales_order" in table_names
    assert "staff" in table_names


def test_dataframe_moficiation_removes_create_at_collum():
    assert "created_at" in list(dataframe_1.columns.values)
    result = dataframe_modification(test_dict_list)
    assert "created_at" not in list(result[0]["sales_order"].columns.values)

def test_dataframe_modification_removes_columns_from_all_items_in_multi_item_list():
    test_dict_list = [{"sales_order": dataframe_1}, {"staff": staff_data}]
    for item in test_dict_list:
        target_dataframe= list(item.values())[0]
        assert "created_at" in list(target_dataframe.columns.values)
    result = dataframe_modification(test_dict_list)
    for item in result:
        target_dataframe= list(item.values())[0]
        assert "created_at" not in list(target_dataframe.columns.values)


def test_dataframe_moficiation_removes_last_updated_column():
    assert "last_updated" in list(dataframe_1.columns.values)
    result = dataframe_modification(test_dict_list)
    assert "last_updated" not in list(result[0]["sales_order"].columns.values)

def test_dataframe_modification_removes_columns_from_all_items_in_multi_item_list():
    test_dict_list = [{"sales_order": dataframe_1}, {"staff": staff_data}]
    for item in test_dict_list:
        target_dataframe= list(item.values())[0]
        assert "last_updated" in list(target_dataframe.columns.values)
    result = dataframe_modification(test_dict_list)
    for item in result:
        target_dataframe= list(item.values())[0]
        assert "last_updated" not in list(target_dataframe.columns.values)


def test_dataframe_modification_raises_error_if_missing_columns():
    missing_string_1 = {
        "sales_order_id": [2, 3],
        "design_id": [3, 4],
        "staff_id": [19, 10],
        "counterparty_id": [8, 4],
        "units_sold": [42972, 65839],
        "unit_price": ["3.94", "2.91"],
        "currency_id": [2, 3],
        "agreed_delivery_date": ["2022-11-07", "2022-11-06"],
        "agreed_payment_date": ["2022-11-08", "2022-11-07"],
        "agreed_delivery_location_id": [8, 19],
    }
    missing_dataframe_1 = pd.DataFrame(missing_string_1)
    missing_values_table = [{"missing_values": missing_dataframe_1}]
    with pytest.raises(KeyError):
        dataframe_modification(missing_values_table)

