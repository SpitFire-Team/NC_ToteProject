
star_schema_ref = {
    "fact_sales_order": [
        "sales_record_id",
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id"
    ],
    "fact_purchase_order": [
        "purchase_record_id",
        "purchase_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "staff_id",
        "counterparty_id",
        "item_code",
        "item_quantity",
        "item_unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id"
    ],
    "fact_payment": [
        "payment_record_id",
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"
    ],
    "dim_date": [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter"
    ],
    "dim_staff": [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address"
    ],
    "dim_location": [
        "location_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone"
    ],
    "dim_currency": [
        "currency_id",
        "currency_code",
        "currency_name"
    ],
    "dim_design": [
        "design_id",
        "design_name",
        "file_location",
        "file_name"
    ],
    "dim_counterparty": [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number"
    ],
    "dim_payment_type": [
        "payment_type_id",
        "payment_type_name"
    ],
    "dim_transaction": [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id"
    ]
}
