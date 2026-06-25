from structure.app.dsl.api import SchemaMode


def test_hooks_attach_to_declared_subtransform_boundaries(orders_recipe) -> None:
    """I can attach a hook to a subtransform using @before(method, lane=lane) or @after(method, lane=lane)."""

    assert [hook.name for hook in orders_recipe.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in orders_recipe.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in orders_recipe.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in orders_recipe.steps[4].after_hooks] == ["add_quality_columns"]


def test_hooks_record_input_access_and_projection_validation_contracts(orders_recipe) -> None:
    """I can opt a hook into original input access."""

    lookup = orders_recipe.steps[3].after_hooks[0]
    quality = orders_recipe.steps[4].after_hooks[0]

    assert lookup.pass_inputs
    assert lookup.schema_mode is SchemaMode.ALLOW_EXTRA_COLUMNS
    assert quality.project_output
    assert [
        (validation.reason, validation.mode, validation.project) for validation in orders_recipe.steps[4].validations
    ] == [
        ("hook", SchemaMode.ALLOW_EXTRA_COLUMNS, True),
        ("hook_projected", SchemaMode.STRICT, False),
    ]


def test_generated_code_calls_source_transform_hooks_directly(orders_transform_text) -> None:
    """Generated code calls hooks directly on the source transform instance."""

    assert "        self._impl = EnrichOrders()" in orders_transform_text
    assert "        orders = self._impl.use_current_orders(" in orders_transform_text
    assert "        orders = self._impl.remove_negative_totals(" in orders_transform_text
    assert "        orders = self._impl.note_lookup_inputs(" in orders_transform_text
    assert "        published = self._impl.add_quality_columns(" in orders_transform_text
