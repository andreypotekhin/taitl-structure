from structure import Array, Boolean, Date, Decimal, Double, Integer, Long, Map, String, Schema, field


class OrderRevenueRollup(Schema):
    tenant_id = field(String(), nullable=True)
    product_category = field(String(), nullable=True)
    order_date = field(Date(), nullable=True)
    grouping_id = field(Integer(), nullable=False)
    category_subtotal = field(Boolean(), nullable=True)
    order_count = field(Long(), nullable=False)
    large_order_count = field(Long(), nullable=False)
    large_units = field(Long(), nullable=True)
    any_large_order = field(Boolean(), nullable=True)
    all_large_orders = field(Boolean(), nullable=True)
    quantity_stddev = field(Double(), nullable=True)
    quantity_variance = field(Double(), nullable=True)
    quantity_median = field(Long(), nullable=True)
    quantity_total = field(Long(), nullable=False)
    quantity_price_corr = field(Double(), nullable=True)
    quantity_price_covar = field(Double(), nullable=True)
    estimated_customers = field(Long(), nullable=False)
    first_customer_id = field(String(), nullable=True)
    last_customer_id = field(String(), nullable=True)
    customer_ids = field(Array(String(), contains_null=False), nullable=True)
    order_ids = field(Array(String(), contains_null=False), nullable=True)


class OrderProductCube(Schema):
    tenant_id = field(String(), nullable=True)
    product_category = field(String(), nullable=True)
    customer_tier = field(String(), nullable=True)
    grouping_id = field(Integer(), nullable=False)
    order_count = field(Long(), nullable=False)
    distinct_customers = field(Long(), nullable=False)
    gross_total = field(Decimal(12, 2), nullable=False)


class OrderCustomerWindow(Schema):
    tenant_id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    order_id = field(String(), nullable=False)
    quantity = field(Long(), nullable=False)
    percent_rank = field(Double(), nullable=False)
    cume_dist = field(Double(), nullable=False)
    quantity_tile = field(Integer(), nullable=False)
    first_order_id = field(String(), nullable=True)
    last_order_id = field(String(), nullable=True)
    second_order_id = field(String(), nullable=True)
    running_units = field(Long(), nullable=False)
    running_avg_units = field(Double(), nullable=True)
    running_min_units = field(Long(), nullable=False)
    running_max_units = field(Long(), nullable=False)
    running_order_count = field(Long(), nullable=False)


class OrderCollectionSource(Schema):
    id = field(String(), nullable=False)
    tags = field(Array(String(), contains_null=False), nullable=True)
    nested_tags = field(Array(Array(String(), contains_null=False), contains_null=False), nullable=True)
    scores = field(Array(Integer(), contains_null=False), nullable=True)
    attributes = field(Map(String(), String()), nullable=True)


class OrderCollectionProfile(Schema):
    id = field(String(), nullable=False)
    normalized_tags = field(Array(String(), contains_null=True), nullable=True)
    sorted_tags = field(Array(String(), contains_null=False), nullable=True)
    flat_tags = field(Array(String(), contains_null=False), nullable=True)
    score_total = field(Integer(), nullable=True)
    tag_position = field(Long(), nullable=True)
    has_priority = field(Boolean(), nullable=True)
    all_tags_present = field(Boolean(), nullable=True)
    normalized_attributes = field(Map(String(), String()), nullable=True)
    zipped_attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)
    attribute_keys = field(Array(String(), contains_null=False), nullable=True)
    attribute_values = field(Array(String(), contains_null=False), nullable=True)
    roundtrip_attributes = field(Map(String(), String()), nullable=True)
